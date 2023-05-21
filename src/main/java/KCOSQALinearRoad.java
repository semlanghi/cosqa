import annotation.AnnotationAwareTimeWindows;
import annotation.ConsistencyAnnotatedRecord;
import annotation.constraint.ConstraintFactory;
import annotation.constraint.StreamingConstraint;
import annotation.polynomial.Monomial;
import gps.GPS;
import gps.GPSSerde;
import gps.SpeedConstraintGPSValueFactory;
import linearroad.SpeedConstraintLinearRoadValueFactory;
import linearroad.SpeedEvent;
import linearroad.SpeedEventSerde;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.*;
import org.apache.kafka.streams.state.internals.InMemoryKeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.internals.ValueAndTimestampSerde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stocks.Stock;
import stocks.StockSerde;
import topkstreaming.CADistanceBasedRanker;
import topkstreaming.InMemoryTopKKeyValueStore;
import topkstreaming.Ranker;
import topkstreaming.TopKCAProcessor;
import utils.ApplicationSupplier;
import utils.ExperimentConfig;
import utils.PerformanceProcessor;

import java.time.Duration;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static annotation.AnnKStream.ANNOTATE_NAME;
import static annotation.AnnWindowedTableImpl.TOP_K_NAME;


public class KCOSQALinearRoad {
    public static void main(String[] args){

        Logger logger = LoggerFactory.getLogger(KCOSQALinearRoad.class);
        Properties props = new Properties();
        String appID = UUID.randomUUID().toString();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Serdes.Long().deserializer().getClass());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SpeedEventSerde.SpeedEventDeserializer.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpeedEventSerde.instance().getClass());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        props.put(StreamsConfig.topicPrefix(TopicConfig.SEGMENT_BYTES_CONFIG), Integer.MAX_VALUE);
        props.put(StreamsConfig.topicPrefix(TopicConfig.SEGMENT_MS_CONFIG), Long.MAX_VALUE);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);


        props.put(ExperimentConfig.CONSTRAINT_STRICTNESS, args[0]);
        props.put(ExperimentConfig.INCONSISTENCY_PERCENTAGE, args[1]);
        props.put(ExperimentConfig.WINDOW_SIZE_MS, args[2]);
        props.put(ExperimentConfig.WINDOW_SLIDE_MS, args[3]);
        props.put(ExperimentConfig.RESULT_FILE_DIR, args[4]);
        props.put(ExperimentConfig.EVENTS_MAX, args[5]);
        props.put(ExperimentConfig.EVENTS_GRANULARITY, args[6]);

        props.put(ExperimentConfig.RESULT_FILE_SUFFIX, "kcosqa-linearroad");

        int constraintStrictness = Integer.parseInt(props.getProperty(ExperimentConfig.CONSTRAINT_STRICTNESS));
        Duration size = Duration.ofMillis(Long.parseLong(props.getProperty(ExperimentConfig.WINDOW_SIZE_MS)));
        Duration advance = Duration.ofMillis(Long.parseLong(props.getProperty(ExperimentConfig.WINDOW_SLIDE_MS)));
        TimeWindows timeWindows = TimeWindows.ofSizeAndGrace(size, size).advanceBy(advance);
        AnnotationAwareTimeWindows annotationAwareTimeWindows = AnnotationAwareTimeWindows.ofSizeAndGrace(size, size)
                .advanceBy(advance);
        JoinWindows joinWindows = JoinWindows.ofTimeDifferenceAndGrace(Duration.ofMillis(timeWindows.size()/2), size)
                .after(Duration.ZERO).before(size);
        String topic = args[7] + "-incons-" +props.getProperty(ExperimentConfig.INCONSISTENCY_PERCENTAGE);
        int threshold = 1;

        //Building a SpeedEvent stream within the ValueAndTimestamp Object
        StreamsBuilder builder = new StreamsBuilder();
        KStream<Long, SpeedEvent> SpeedEventKStream = builder
                .stream(topic, Consumed.with(Serdes.Long(), SpeedEventSerde.instance(),new TimestampExtractor() {
                    @Override
                    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
                        return ((SpeedEvent)record.value()).getTimestamp();
                    }
                }, Topology.AutoOffsetReset.EARLIEST));


        Materialized<ValueAndTimestamp<SpeedEvent>, ConsistencyAnnotatedRecord<ValueAndTimestamp<SpeedEvent>>, KeyValueStore<Bytes, byte[]>> annotatedRecordsState =
                Materialized.as(new InMemoryKeyValueBytesStoreSupplier(ANNOTATE_NAME + "-baseline"));
        annotatedRecordsState.withKeySerde(new ValueAndTimestampSerde<>(SpeedEventSerde.instance()));
        annotatedRecordsState.withValueSerde(ConsistencyAnnotatedRecord.serde(SpeedEventSerde.instance()));
        annotatedRecordsState.withCachingDisabled();
        annotatedRecordsState.withLoggingDisabled();


        ConstraintFactory<ValueAndTimestamp<SpeedEvent>> speedConstraintSpeedEventValueFactory = new SpeedConstraintLinearRoadValueFactory(0.01/constraintStrictness, -0.01/constraintStrictness);
        KStream<Long, ValueAndTimestamp<SpeedEvent>> aggregatedStream = SpeedEventKStream
                .transformValues(new ValueTransformerSupplier<>() {
                    @Override
                    public ValueTransformer<SpeedEvent, ValueAndTimestamp<SpeedEvent>> get() {
                        return new ValueTransformer<>() {
                            ProcessorContext context;

                            @Override
                            public void init(ProcessorContext context) {
                                this.context = context;
                            }

                            @Override
                            public ValueAndTimestamp<SpeedEvent> transform(SpeedEvent value) {
                                return ValueAndTimestamp.make(value, context.timestamp());
                            }

                            @Override
                            public void close() {

                            }
                        };
                    }
                });

//
//        TimeWindows timeWindows = TimeWindows.ofSizeAndGrace(Duration.ofMillis(1000*5), Duration.ofMillis(1000*5)).advanceBy(Duration.ofMillis(1000*1));
//        AnnotationAwareTimeWindows annotationAwareTimeWindows = AnnotationAwareTimeWindows.ofSizeAndGrace(Duration.ofMillis(1000*5), Duration.ofMillis(1000*5)).advanceBy(Duration.ofMillis(1000*1));
//        JoinWindows joinWindows = JoinWindows.ofTimeDifferenceAndGrace(Duration.ofMillis(timeWindows.size()/2), Duration.ofMillis(1000*5)).after(Duration.ZERO).before(Duration.ofMillis(timeWindows.size()));

        Materialized<ValueAndTimestamp<SpeedEvent>, ConsistencyAnnotatedRecord<ValueAndTimestamp<SpeedEvent>>, WindowStore<Bytes, byte[]>> annotatedRecordWindowStoreMaterialized = Materialized.
                <ValueAndTimestamp<SpeedEvent>, ConsistencyAnnotatedRecord<ValueAndTimestamp<SpeedEvent>>>as(Stores.inMemoryWindowStore("reduce-annot-state-store"+appID, Duration.ofMillis(annotationAwareTimeWindows.size() + annotationAwareTimeWindows.gracePeriodMs()), Duration.ofMillis(annotationAwareTimeWindows.size()), false))
                .withKeySerde(new ValueAndTimestampSerde(SpeedEventSerde.instance()))
                .withValueSerde(ConsistencyAnnotatedRecord.serde(SpeedEventSerde.instance()));

        StreamJoined<Long, ValueAndTimestamp<SpeedEvent>, ValueAndTimestamp<SpeedEvent>> joined = StreamJoined.with(Serdes.Long(), new ValueAndTimestampSerde(SpeedEventSerde.instance()), new ValueAndTimestampSerde(SpeedEventSerde.instance()))
                .withThisStoreSupplier(Stores.inMemoryWindowStore("join1-annot-" + UUID.randomUUID(), Duration.ofMillis(timeWindows.size() + timeWindows.gracePeriodMs()), Duration.ofMillis(timeWindows.size()), true))
                .withOtherStoreSupplier(Stores.inMemoryWindowStore("join2-annot-" + UUID.randomUUID(), Duration.ofMillis(timeWindows.size() + timeWindows.gracePeriodMs()), Duration.ofMillis(timeWindows.size()), true));
        KStream<Long, ConsistencyAnnotatedRecord<ValueAndTimestamp<SpeedEvent>>> annotatedKStream = aggregatedStream
                .join(aggregatedStream, new ValueJoiner<ValueAndTimestamp<SpeedEvent>, ValueAndTimestamp<SpeedEvent>, ConsistencyAnnotatedRecord<ValueAndTimestamp<SpeedEvent>>>() {
                            @Override
                            public ConsistencyAnnotatedRecord<ValueAndTimestamp<SpeedEvent>> apply(ValueAndTimestamp<SpeedEvent> value1, ValueAndTimestamp<SpeedEvent> value2) {
                                StreamingConstraint<ValueAndTimestamp<SpeedEvent>> speedConstraint = speedConstraintSpeedEventValueFactory.make(value2);
                                double result = speedConstraint.checkConstraint(value1);

                                ConsistencyAnnotatedRecord<ValueAndTimestamp<SpeedEvent>> tmp =
                                        new ConsistencyAnnotatedRecord<>(value1);
                                if (result < 0 || result > 0) {
                                    tmp.setPolynomial(tmp.getPolynomial().times(value2, (int) Math.ceil(Math.abs(result))));
                                }
                                return tmp;
                            }
                        }, joinWindows
                        , joined)
                .selectKey(new KeyValueMapper<Long, ConsistencyAnnotatedRecord<ValueAndTimestamp<SpeedEvent>>, ValueAndTimestamp<SpeedEvent>>() {
                    @Override
                    public ValueAndTimestamp<SpeedEvent> apply(Long key, ConsistencyAnnotatedRecord<ValueAndTimestamp<SpeedEvent>> value) {
                        return value.getWrappedRecord();
                    }
                })
                .groupByKey(Grouped.with(new ValueAndTimestampSerde<>(SpeedEventSerde.instance()), ConsistencyAnnotatedRecord.serde(SpeedEventSerde.instance())))
                .windowedBy(annotationAwareTimeWindows)
                .reduce(new Reducer<ConsistencyAnnotatedRecord<ValueAndTimestamp<SpeedEvent>>>() {
                    @Override
                    public ConsistencyAnnotatedRecord<ValueAndTimestamp<SpeedEvent>> apply(ConsistencyAnnotatedRecord<ValueAndTimestamp<SpeedEvent>> value1, ConsistencyAnnotatedRecord<ValueAndTimestamp<SpeedEvent>> value2) {
                        return value1.withPolynomial(value1.getPolynomial().times(value2.getPolynomial()));
                    }
                }, annotatedRecordWindowStoreMaterialized).mapValues(new ValueMapperWithKey<Windowed<ValueAndTimestamp<SpeedEvent>>, ConsistencyAnnotatedRecord<ValueAndTimestamp<SpeedEvent>>, ConsistencyAnnotatedRecord<ValueAndTimestamp<SpeedEvent>>>() {
                    @Override
                    public ConsistencyAnnotatedRecord<ValueAndTimestamp<SpeedEvent>> apply(Windowed<ValueAndTimestamp<SpeedEvent>> readOnlyKey, ConsistencyAnnotatedRecord<ValueAndTimestamp<SpeedEvent>> value) {
                        Iterator<Monomial> iterator = value.getPolynomial().getMonomials().iterator();
                        while (iterator.hasNext())
                            iterator.next().slide(readOnlyKey.window());
                        return value;
                    }
                }).toStream().selectKey(new KeyValueMapper<Windowed<ValueAndTimestamp<SpeedEvent>>, ConsistencyAnnotatedRecord<ValueAndTimestamp<SpeedEvent>>, Long>() {
                    @Override
                    public Long apply(Windowed<ValueAndTimestamp<SpeedEvent>> key, ConsistencyAnnotatedRecord<ValueAndTimestamp<SpeedEvent>> value) {
                        return key.key().value().getVid();
                    }
                });

        ApplicationSupplier applicationSupplier = new ApplicationSupplier(1);

        //Processing pipeline of the annotated stream
        KStream<Integer, ConsistencyAnnotatedRecord<ValueAndTimestamp<SpeedEvent>>> annotatedTimestampedKStream = annotatedKStream
                .selectKey((key, value) -> value.getWrappedRecord().value().getSegment());

        ValueJoiner<SpeedEvent, SpeedEvent, Pair<SpeedEvent, SpeedEvent>> internalValueJoiner = new ValueJoiner<>() {
            @Override
            public Pair<SpeedEvent, SpeedEvent> apply(SpeedEvent value1, SpeedEvent value2) {
                if (value1.getxWay()!=value2.getxWay())
                    return null;
                return new ImmutablePair<>(value1, value2);
            }
        };
        KStream<Integer, ConsistencyAnnotatedRecord<ValueAndTimestamp<Pair<SpeedEvent, SpeedEvent>>>> joinedStream = annotatedTimestampedKStream.join(annotatedTimestampedKStream, (value1, value2) -> {
                    ValueAndTimestamp<Pair<SpeedEvent, SpeedEvent>> valueAndTimestamp = ValueAndTimestamp.make(internalValueJoiner.apply(value1.getWrappedRecord().value(), value2.getWrappedRecord().value())
                            , Math.max(value1.getWrappedRecord().timestamp(), value2.getWrappedRecord().timestamp()));
                    return new ConsistencyAnnotatedRecord<>(value1.getPolynomial().times(value2.getPolynomial()), valueAndTimestamp);
                }, joinWindows, StreamJoined.with(Serdes.Integer(), ConsistencyAnnotatedRecord.serde(SpeedEventSerde.instance()), ConsistencyAnnotatedRecord.serde(SpeedEventSerde.instance()))
                        .withThisStoreSupplier(Stores.inMemoryWindowStore("join1"+ UUID.randomUUID(), Duration.ofMillis(joinWindows.size() + joinWindows.gracePeriodMs()), Duration.ofMillis(joinWindows.size()), true))
                        .withOtherStoreSupplier(Stores.inMemoryWindowStore("join2"+UUID.randomUUID(), Duration.ofMillis(joinWindows.size() + joinWindows.gracePeriodMs()), Duration.ofMillis(joinWindows.size()), true)))
                .filter(new Predicate<Integer, ConsistencyAnnotatedRecord<ValueAndTimestamp<Pair<SpeedEvent, SpeedEvent>>>>() {
                    @Override
                    public boolean test(Integer key, ConsistencyAnnotatedRecord<ValueAndTimestamp<Pair<SpeedEvent, SpeedEvent>>> value) {
                        if (value.getWrappedRecord()!=null)
                            return true;
                        else return false;
                    }
                });
//                .filter(new Predicate<Integer, ConsistencyAnnotatedRecord<ValueAndTimestamp<Pair<SpeedEvent, SpeedEvent>>>>() {
//                    @Override
//                    public boolean test(Integer key, ConsistencyAnnotatedRecord<ValueAndTimestamp<Pair<SpeedEvent, SpeedEvent>>> value) {
//                        return value.getPolynomial().getMonomialsDegreeSum() > threshold;
//                    }
//                });

        joinedStream.process(new ProcessorSupplier<Integer, ConsistencyAnnotatedRecord<ValueAndTimestamp<Pair<SpeedEvent, SpeedEvent>>>, Void, Void>() {
            @Override
            public Processor<Integer, ConsistencyAnnotatedRecord<ValueAndTimestamp<Pair<SpeedEvent, SpeedEvent>>>, Void, Void> get() {
                return new PerformanceProcessor<>(applicationSupplier, props);
            }
        });

        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            Runtime.getRuntime().halt(0);
        }));

        streams.setStateListener((newState, oldState) -> {
            if (KafkaStreams.State.PENDING_SHUTDOWN.equals(newState)) {
                try {
                    // setupSchemas a timer, so if nice exit fails, the nasty exit happens
                    //sendOut("END");
                    Thread.sleep(6000);
                    Runtime.getRuntime().exit(0);
                } catch (Throwable ex) {
                    // exit nastily if we have a problem
                    Runtime.getRuntime().halt(-1);
                } finally {
                    // should never get here
                    Runtime.getRuntime().halt(-1);
                }
            }
        });

        applicationSupplier.setApp(streams);

        streams.start();
    }
}
