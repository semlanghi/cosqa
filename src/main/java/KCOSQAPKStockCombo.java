import annotation.AnnotationAwareTimeWindows;
import annotation.ConsistencyAnnotatedRecord;
import annotation.constraint.ConstraintFactory;
import annotation.constraint.StreamingConstraint;
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
import org.apache.kafka.streams.state.*;
import org.apache.kafka.streams.state.internals.InMemoryKeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.internals.ValueAndTimestampSerde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stocks.PKStockValueFactory;
import stocks.SpeedConstraintStockValueFactory;
import stocks.Stock;
import stocks.StockSerde;
import topkstreaming.CADistanceBasedRanker;
import topkstreaming.InMemoryTopKKeyValueStore;
import topkstreaming.Ranker;
import topkstreaming.TopKCAProcessorNotWindowed;
import utils.ApplicationSupplier;
import utils.ExperimentConfig;
import utils.PerformanceInputInconsistencyTransformer;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static annotation.AnnKStream.ANNOTATE_NAME;
import static annotation.AnnWindowedTableImpl.TOP_K_NAME;


public class KCOSQAPKStockCombo {
    public static void main(String[] args){

        Logger logger = LoggerFactory.getLogger(KCOSQAPKStockCombo.class);
        Properties props = new Properties();
        String appID = UUID.randomUUID().toString();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Serdes.String().deserializer().getClass());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StockSerde.StockDeserializer.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Double().getClass());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
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

        props.put(ExperimentConfig.RESULT_FILE_SUFFIX, "kcosqa-stock-combo-pk");

        int constraintStrictness = Integer.parseInt(props.getProperty(ExperimentConfig.CONSTRAINT_STRICTNESS));
        Duration size = Duration.ofMillis(Long.parseLong(props.getProperty(ExperimentConfig.WINDOW_SIZE_MS)));
        Duration advance = Duration.ofMillis(Long.parseLong(props.getProperty(ExperimentConfig.WINDOW_SLIDE_MS)));
        TimeWindows timeWindows = TimeWindows.ofSizeAndGrace(size, size).advanceBy(advance);
        AnnotationAwareTimeWindows annotationAwareTimeWindows = AnnotationAwareTimeWindows.ofSizeAndGrace(size, size)
                .advanceBy(advance);
        JoinWindows joinWindows = JoinWindows.ofTimeDifferenceAndGrace(Duration.ofMillis(timeWindows.size()/2), size)
                .after(Duration.ZERO).before(size);
        String topic = args[7];
        ApplicationSupplier applicationSupplier = new ApplicationSupplier(1);


        //Building a stock stream within the ValueAndTimestamp Object
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, Stock> stockKStream = builder
                .stream(topic, Consumed.with(Serdes.String(), StockSerde.instance(),new TimestampExtractor() {
                    @Override
                    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
                        return ((Stock)record.value()).getTs();
                    }
                }, Topology.AutoOffsetReset.EARLIEST));


        Materialized<ValueAndTimestamp<Stock>, ConsistencyAnnotatedRecord<ValueAndTimestamp<Stock>>, KeyValueStore<Bytes, byte[]>> annotatedRecordsState =
                Materialized.as(new InMemoryKeyValueBytesStoreSupplier(ANNOTATE_NAME + "-baseline"));
        annotatedRecordsState.withKeySerde(new ValueAndTimestampSerde<>(StockSerde.instance()));
        annotatedRecordsState.withValueSerde(ConsistencyAnnotatedRecord.serde(StockSerde.instance()));
        annotatedRecordsState.withCachingDisabled();
        annotatedRecordsState.withLoggingDisabled();


        ConstraintFactory<ValueAndTimestamp<Stock>> speedConstraintStockValueFactory = new PKStockValueFactory();
        KStream<String, ValueAndTimestamp<Stock>> aggregatedStream = stockKStream
                .transformValues(new ValueTransformerSupplier<>() {
                    @Override
                    public ValueTransformer<Stock, ValueAndTimestamp<Stock>> get() {
                        return new ValueTransformer<>() {
                            ProcessorContext context;

                            @Override
                            public void init(ProcessorContext context) {
                                this.context = context;
                            }

                            @Override
                            public ValueAndTimestamp<Stock> transform(Stock value) {
                                return ValueAndTimestamp.make(value, context.timestamp());
                            }

                            @Override
                            public void close() {

                            }
                        };
                    }
                });

//
//        TimeWindows timeWindows = TimeWindows.ofSizeWithNoGrace(Duration.ofDays(5)).advanceBy(Duration.ofDays(1));
//        JoinWindows joinWindows = JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMillis(timeWindows.size()/2)).after(Duration.ZERO).before(Duration.ofMillis(timeWindows.size()));
//        AnnotationAwareTimeWindows annotationAwareTimeWindows = AnnotationAwareTimeWindows.ofSizeAndGrace(Duration.ofDays(5), Duration.ofDays(5)).advanceBy(Duration.ofDays(1));

        Materialized<ValueAndTimestamp<Stock>, ConsistencyAnnotatedRecord<ValueAndTimestamp<Stock>>, WindowStore<Bytes, byte[]>> annotatedRecordWindowStoreMaterialized = Materialized.
                <ValueAndTimestamp<Stock>, ConsistencyAnnotatedRecord<ValueAndTimestamp<Stock>>>as(Stores.inMemoryWindowStore("reduce-annot-state-store"+appID, Duration.ofMillis(annotationAwareTimeWindows.size() + annotationAwareTimeWindows.gracePeriodMs()), Duration.ofMillis(annotationAwareTimeWindows.size()), false))
                .withKeySerde(new ValueAndTimestampSerde(StockSerde.instance()))
                .withValueSerde(ConsistencyAnnotatedRecord.serde(StockSerde.instance()));

        StreamJoined<String, ValueAndTimestamp<Stock>, ValueAndTimestamp<Stock>> joined = StreamJoined.with(Serdes.String(), new ValueAndTimestampSerde(StockSerde.instance()), new ValueAndTimestampSerde(StockSerde.instance()))
                .withThisStoreSupplier(Stores.inMemoryWindowStore("join1-annot-" + UUID.randomUUID(), Duration.ofMillis(timeWindows.size() + timeWindows.gracePeriodMs()), Duration.ofMillis(timeWindows.size()), true))
                .withOtherStoreSupplier(Stores.inMemoryWindowStore("join2-annot-" + UUID.randomUUID(), Duration.ofMillis(timeWindows.size() + timeWindows.gracePeriodMs()), Duration.ofMillis(timeWindows.size()), true));
        KStream<String, ConsistencyAnnotatedRecord<ValueAndTimestamp<Stock>>> annotatedKStream = aggregatedStream
                .join(aggregatedStream, new ValueJoiner<ValueAndTimestamp<Stock>, ValueAndTimestamp<Stock>, ConsistencyAnnotatedRecord<ValueAndTimestamp<Stock>>>() {
                            @Override
                            public ConsistencyAnnotatedRecord<ValueAndTimestamp<Stock>> apply(ValueAndTimestamp<Stock> value1, ValueAndTimestamp<Stock> value2) {
                                StreamingConstraint<ValueAndTimestamp<Stock>> speedConstraint = speedConstraintStockValueFactory.make(value2);
                                double result = speedConstraint.checkConstraint(value1);

                                ConsistencyAnnotatedRecord<ValueAndTimestamp<Stock>> tmp =
                                        new ConsistencyAnnotatedRecord<>(value1);
                                if (result < 0 || result > 0) {
                                    tmp.setPolynomial(tmp.getPolynomial().times(value2, (int) Math.ceil(Math.abs(result))));
                                }
                                return tmp;
                            }
                        }, joinWindows
                        , joined)
                .selectKey(new KeyValueMapper<String, ConsistencyAnnotatedRecord<ValueAndTimestamp<Stock>>, ValueAndTimestamp<Stock>>() {
                    @Override
                    public ValueAndTimestamp<Stock> apply(String key, ConsistencyAnnotatedRecord<ValueAndTimestamp<Stock>> value) {
                        return value.getWrappedRecord();
                    }
                })
                .groupByKey(Grouped.with(new ValueAndTimestampSerde<>(StockSerde.instance()), ConsistencyAnnotatedRecord.serde(StockSerde.instance())))
                .windowedBy(annotationAwareTimeWindows)
                .reduce(new Reducer<ConsistencyAnnotatedRecord<ValueAndTimestamp<Stock>>>() {
                    @Override
                    public ConsistencyAnnotatedRecord<ValueAndTimestamp<Stock>> apply(ConsistencyAnnotatedRecord<ValueAndTimestamp<Stock>> value1, ConsistencyAnnotatedRecord<ValueAndTimestamp<Stock>> value2) {
                        return value1.withPolynomial(value1.getPolynomial().times(value2.getPolynomial()));
                    }
                }, annotatedRecordWindowStoreMaterialized)
                .toStream().selectKey(new KeyValueMapper<Windowed<ValueAndTimestamp<Stock>>, ConsistencyAnnotatedRecord<ValueAndTimestamp<Stock>>, String>() {
                    @Override
                    public String apply(Windowed<ValueAndTimestamp<Stock>> key, ConsistencyAnnotatedRecord<ValueAndTimestamp<Stock>> value) {
                        return key.key().value().getName().toString();
                    }
                }).transform(new TransformerSupplier<String, ConsistencyAnnotatedRecord<ValueAndTimestamp<Stock>>, KeyValue<String, ConsistencyAnnotatedRecord<ValueAndTimestamp<Stock>>>>() {
                    @Override
                    public Transformer<String, ConsistencyAnnotatedRecord<ValueAndTimestamp<Stock>>, KeyValue<String, ConsistencyAnnotatedRecord<ValueAndTimestamp<Stock>>>> get() {
                        return new PerformanceInputInconsistencyTransformer<>(applicationSupplier, props);
                    }
                });





        //Processing pipeline of the annotated stream

        JoinWindows joinWindows1 = JoinWindows.ofTimeDifferenceAndGrace(Duration.ofMillis(50000), Duration.ofMillis(100000))
                .after(Duration.ZERO).before(Duration.ofMillis(100000));
        KStream<Long, ConsistencyAnnotatedRecord<ValueAndTimestamp<Stock>>> annotatedTimestampedKStream = annotatedKStream
                .selectKey((key, value) -> value.getWrappedRecord().timestamp());

        ValueJoiner<Stock, Stock, Pair<Stock, Stock>> internalValueJoiner = new ValueJoiner<>() {
            @Override
            public Pair<Stock, Stock> apply(Stock value1, Stock value2) {
                if (value1.getName().equals(value2.getName()))
                    return null;
                return new ImmutablePair<>(value1, value2);
            }
        };
        KStream<Long, ConsistencyAnnotatedRecord<ValueAndTimestamp<Pair<Stock, Stock>>>> joinedStream = annotatedTimestampedKStream.join(annotatedTimestampedKStream, (value1, value2) -> {
            ValueAndTimestamp<Pair<Stock, Stock>> valueAndTimestamp = ValueAndTimestamp.make(internalValueJoiner.apply(value1.getWrappedRecord().value(), value2.getWrappedRecord().value())
                    , Math.max(value1.getWrappedRecord().timestamp(), value2.getWrappedRecord().timestamp()));
            return new ConsistencyAnnotatedRecord<>(value1.getPolynomial().times(value2.getPolynomial()), valueAndTimestamp);
        }, joinWindows1, StreamJoined.with(Serdes.Long(), ConsistencyAnnotatedRecord.serde(StockSerde.instance()), ConsistencyAnnotatedRecord.serde(StockSerde.instance()))
                .withThisStoreSupplier(Stores.inMemoryWindowStore("join1"+ UUID.randomUUID(), Duration.ofMillis(joinWindows1.size() + joinWindows1.gracePeriodMs()), Duration.ofMillis(joinWindows1.size()), true))
                .withOtherStoreSupplier(Stores.inMemoryWindowStore("join2"+UUID.randomUUID(), Duration.ofMillis(joinWindows1.size() + joinWindows1.gracePeriodMs()), Duration.ofMillis(joinWindows1.size()), true)))
                .filter(new Predicate<Long, ConsistencyAnnotatedRecord<ValueAndTimestamp<Pair<Stock, Stock>>>>() {
                    @Override
                    public boolean test(Long key, ConsistencyAnnotatedRecord<ValueAndTimestamp<Pair<Stock, Stock>>> value) {
                        if (value.getWrappedRecord()!=null)
                            return true;
                        else return false;
                    }
                });

//        joinedStream.process(new ProcessorSupplier<Long, ConsistencyAnnotatedRecord<ValueAndTimestamp<Pair<Stock, Stock>>>, Void, Void>() {
//            @Override
//            public Processor<Long, ConsistencyAnnotatedRecord<ValueAndTimestamp<Pair<Stock, Stock>>>, Void, Void> get() {
//                return new PerformanceProcessor<>(applicationSupplier, props);
//            }
//        });


        CADistanceBasedRanker ranker = new CADistanceBasedRanker(3, Ranker.Order.DESCENDING);

        builder.addStateStore(new StoreBuilder<>() {
            @Override
            public StoreBuilder<StateStore> withCachingEnabled() {
                return null;
            }

            @Override
            public StoreBuilder<StateStore> withCachingDisabled() {
                return null;
            }

            @Override
            public StoreBuilder<StateStore> withLoggingEnabled(Map<String, String> config) {
                return null;
            }

            @Override
            public StoreBuilder<StateStore> withLoggingDisabled() {
                return null;
            }

            @Override
            public StateStore build() {
                return new InMemoryTopKKeyValueStore<>(ranker.comparator(), timeWindows.sizeMs*10, TOP_K_NAME, ranker.limit());
            }

            @Override
            public Map<String, String> logConfig() {
                return null;
            }

            @Override
            public boolean loggingEnabled() {
                return false;
            }

            @Override
            public String name() {
                return TOP_K_NAME;
            }
        });

        joinedStream.process(new ProcessorSupplier<Long, ConsistencyAnnotatedRecord<ValueAndTimestamp<Pair<Stock, Stock>>>, Void, Void>() {
            @Override
            public Processor<Long, ConsistencyAnnotatedRecord<ValueAndTimestamp<Pair<Stock, Stock>>>, Void, Void> get() {
                return new TopKCAProcessorNotWindowed(TOP_K_NAME, annotationAwareTimeWindows, applicationSupplier, props);
            }
        }, TOP_K_NAME);


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
