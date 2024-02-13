import annotation.AnnotationAwareTimeWindows;
import annotation.ConsistencyAnnotatedRecord;
import annotation.constraint.ConstraintFactory;
import annotation.constraint.StreamingConstraint;
import annotation.polynomial.Monomial;
import annotation.polynomial.MonomialImplString;
import annotation.polynomial.Polynomial;
import gps.GPS;
import linearroad.SchemaConstraintLinearRoadValueFactory;
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
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.internals.InMemoryKeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.internals.ValueAndTimestampSerde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.ApplicationSupplier;
import utils.ExperimentConfig;
import utils.PerformanceInputInconsistencyTransformer;
import utils.PerformanceProcessor;

import java.time.Duration;
import java.util.Iterator;
import java.util.Properties;
import java.util.UUID;

import static annotation.AnnKStream.ANNOTATE_NAME;


public class KCOSQASchemaConstraintLinearRoad {
    public static void main(String[] args){

        Logger logger = LoggerFactory.getLogger(KCOSQASchemaConstraintLinearRoad.class);
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

        props.put(ExperimentConfig.RESULT_FILE_SUFFIX, "kcosqa-linearroad-schema");

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
        ApplicationSupplier applicationSupplier = new ApplicationSupplier(1);


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
        KStream<Long, ConsistencyAnnotatedRecord<ValueAndTimestamp<SpeedEvent>>> aggregatedStream = SpeedEventKStream
                .transformValues(new ValueTransformerSupplier<>() {
                    @Override
                    public ValueTransformer<SpeedEvent, ConsistencyAnnotatedRecord<ValueAndTimestamp<SpeedEvent>>> get() {
                        return new ValueTransformer<>() {
                            ProcessorContext context;
                            SchemaConstraintLinearRoadValueFactory schemaConstraintLinearRoadValueFactory = new SchemaConstraintLinearRoadValueFactory();

                            @Override
                            public void init(ProcessorContext context) {
                                this.context = context;
                            }

                            @Override
                            public ConsistencyAnnotatedRecord<ValueAndTimestamp<SpeedEvent>> transform(SpeedEvent value) {
                                ValueAndTimestamp<SpeedEvent> origin = ValueAndTimestamp.make(value, context.timestamp());
                                StreamingConstraint<ValueAndTimestamp<SpeedEvent>> constraint = schemaConstraintLinearRoadValueFactory.make(origin);
                                // COnstraint true if >0
                                ConsistencyAnnotatedRecord<ValueAndTimestamp<SpeedEvent>> polynomialConsistencyAnnotatedRecord;
                                if (constraint.checkConstraint(origin)<0) {
                                    polynomialConsistencyAnnotatedRecord = new ConsistencyAnnotatedRecord<>(new Polynomial(new MonomialImplString(constraint.getDescription(), 1)), origin);
                                } else{
                                    polynomialConsistencyAnnotatedRecord = new ConsistencyAnnotatedRecord<>(new Polynomial(), origin);
                                }
                                return polynomialConsistencyAnnotatedRecord;
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



        JoinWindows joinWindows1 = JoinWindows.ofTimeDifferenceAndGrace(Duration.ofMillis(50000), Duration.ofMillis(100000))
                .after(Duration.ZERO).before(Duration.ofMillis(100000));

        //Processing pipeline of the annotated stream
        KStream<Integer, ConsistencyAnnotatedRecord<ValueAndTimestamp<SpeedEvent>>> annotatedTimestampedKStream = aggregatedStream
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
                }, joinWindows1, StreamJoined.with(Serdes.Integer(), ConsistencyAnnotatedRecord.serde(SpeedEventSerde.instance()), ConsistencyAnnotatedRecord.serde(SpeedEventSerde.instance()))
                        .withThisStoreSupplier(Stores.inMemoryWindowStore("join1"+ UUID.randomUUID(), Duration.ofMillis(joinWindows1.size() + joinWindows1.gracePeriodMs()), Duration.ofMillis(joinWindows1.size()), true))
                        .withOtherStoreSupplier(Stores.inMemoryWindowStore("join2"+UUID.randomUUID(), Duration.ofMillis(joinWindows1.size() + joinWindows1.gracePeriodMs()), Duration.ofMillis(joinWindows1.size()), true)))
                .transform(new TransformerSupplier<Integer, ConsistencyAnnotatedRecord<ValueAndTimestamp<Pair<SpeedEvent, SpeedEvent>>>, KeyValue<Integer, ConsistencyAnnotatedRecord<ValueAndTimestamp<Pair<SpeedEvent, SpeedEvent>>>>>() {
                    @Override
                    public Transformer<Integer, ConsistencyAnnotatedRecord<ValueAndTimestamp<Pair<SpeedEvent, SpeedEvent>>>, KeyValue<Integer, ConsistencyAnnotatedRecord<ValueAndTimestamp<Pair<SpeedEvent, SpeedEvent>>>>> get() {
                        return new PerformanceInputInconsistencyTransformer<>(applicationSupplier, props);
                    }
                })
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
