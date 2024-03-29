import annotation.AnnotationAwareTimeWindows;
import annotation.ConsistencyAnnotatedRecord;
import annotation.constraint.ConstraintFactory;
import annotation.constraint.StreamingConstraint;
import gps.GPS;
import gps.GPSSerde;
import gps.PKGPSValueFactory;
import gps.SpeedConstraintGPSValueFactory;
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


public class KCOSQAPKGPS {
    public static void main(String[] args){

        Logger logger = LoggerFactory.getLogger(KCOSQAPKGPS.class);
        Properties props = new Properties();
        String appID = UUID.randomUUID().toString();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Serdes.String().deserializer().getClass());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GPSSerde.GPSDeserializer.class);
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

        props.put(ExperimentConfig.RESULT_FILE_SUFFIX, "kcosqa-gps-pk");

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

        //Building a GPS stream within the ValueAndTimestamp Object
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, GPS> GPSKStream = builder
                .stream(topic, Consumed.with(Serdes.String(), GPSSerde.instance(),new TimestampExtractor() {
                    @Override
                    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
                        return ((GPS)record.value()).timestamp();
                    }
                }, Topology.AutoOffsetReset.EARLIEST));


        Materialized<ValueAndTimestamp<GPS>, ConsistencyAnnotatedRecord<ValueAndTimestamp<GPS>>, KeyValueStore<Bytes, byte[]>> annotatedRecordsState =
                Materialized.as(new InMemoryKeyValueBytesStoreSupplier(ANNOTATE_NAME + "-baseline"));
        annotatedRecordsState.withKeySerde(new ValueAndTimestampSerde<>(GPSSerde.instance()));
        annotatedRecordsState.withValueSerde(ConsistencyAnnotatedRecord.serde(GPSSerde.instance()));
        annotatedRecordsState.withCachingDisabled();
        annotatedRecordsState.withLoggingDisabled();


        ConstraintFactory<ValueAndTimestamp<GPS>> pkGPSValueFactory = new PKGPSValueFactory();
        KStream<String, ValueAndTimestamp<GPS>> aggregatedStream = GPSKStream
                .transformValues(new ValueTransformerSupplier<>() {
                    @Override
                    public ValueTransformer<GPS, ValueAndTimestamp<GPS>> get() {
                        return new ValueTransformer<>() {
                            ProcessorContext context;

                            @Override
                            public void init(ProcessorContext context) {
                                this.context = context;
                            }

                            @Override
                            public ValueAndTimestamp<GPS> transform(GPS value) {
                                return ValueAndTimestamp.make(value, context.timestamp());
                            }

                            @Override
                            public void close() {

                            }
                        };
                    }
                });


//        TimeWindows timeWindows = TimeWindows.ofSizeWithNoGrace(Duration.ofDays(5)).advanceBy(Duration.ofDays(1));
//        JoinWindows joinWindows = JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMillis(timeWindows.size()/2)).after(Duration.ZERO).before(Duration.ofMillis(timeWindows.size()));
//        AnnotationAwareTimeWindows annotationAwareTimeWindows = AnnotationAwareTimeWindows.ofSizeAndGrace(Duration.ofDays(5), Duration.ofDays(5)).advanceBy(Duration.ofDays(1));

        Materialized<ValueAndTimestamp<GPS>, ConsistencyAnnotatedRecord<ValueAndTimestamp<GPS>>, WindowStore<Bytes, byte[]>> annotatedRecordWindowStoreMaterialized = Materialized.
                <ValueAndTimestamp<GPS>, ConsistencyAnnotatedRecord<ValueAndTimestamp<GPS>>>as(Stores.inMemoryWindowStore("reduce-annot-state-store"+appID, Duration.ofMillis(annotationAwareTimeWindows.size() + annotationAwareTimeWindows.gracePeriodMs()), Duration.ofMillis(annotationAwareTimeWindows.size()), false))
                .withKeySerde(new ValueAndTimestampSerde(GPSSerde.instance()))
                .withValueSerde(ConsistencyAnnotatedRecord.serde(GPSSerde.instance()));

        StreamJoined<String, ValueAndTimestamp<GPS>, ValueAndTimestamp<GPS>> joined = StreamJoined.with(Serdes.String(), new ValueAndTimestampSerde(GPSSerde.instance()), new ValueAndTimestampSerde(GPSSerde.instance()))
                .withThisStoreSupplier(Stores.inMemoryWindowStore("join1-annot-" + UUID.randomUUID(), Duration.ofMillis(timeWindows.size() + timeWindows.gracePeriodMs()), Duration.ofMillis(timeWindows.size()), true))
                .withOtherStoreSupplier(Stores.inMemoryWindowStore("join2-annot-" + UUID.randomUUID(), Duration.ofMillis(timeWindows.size() + timeWindows.gracePeriodMs()), Duration.ofMillis(timeWindows.size()), true));
        KStream<String, ConsistencyAnnotatedRecord<ValueAndTimestamp<GPS>>> annotatedKStream = aggregatedStream
                .join(aggregatedStream, new ValueJoiner<ValueAndTimestamp<GPS>, ValueAndTimestamp<GPS>, ConsistencyAnnotatedRecord<ValueAndTimestamp<GPS>>>() {
                            @Override
                            public ConsistencyAnnotatedRecord<ValueAndTimestamp<GPS>> apply(ValueAndTimestamp<GPS> value1, ValueAndTimestamp<GPS> value2) {
                                StreamingConstraint<ValueAndTimestamp<GPS>> speedConstraint = pkGPSValueFactory.make(value2);
                                double result = speedConstraint.checkConstraint(value1);

                                ConsistencyAnnotatedRecord<ValueAndTimestamp<GPS>> tmp =
                                        new ConsistencyAnnotatedRecord<>(value1);
                                if (result < 0 || result > 0) {
                                    tmp.setPolynomial(tmp.getPolynomial().times(value2, (int) Math.ceil(Math.abs(result))));
                                }
                                return tmp;
                            }
                        }, joinWindows
                        , joined)
                .selectKey(new KeyValueMapper<String, ConsistencyAnnotatedRecord<ValueAndTimestamp<GPS>>, ValueAndTimestamp<GPS>>() {
                    @Override
                    public ValueAndTimestamp<GPS> apply(String key, ConsistencyAnnotatedRecord<ValueAndTimestamp<GPS>> value) {
                        return value.getWrappedRecord();
                    }
                })
                .groupByKey(Grouped.with(new ValueAndTimestampSerde<>(GPSSerde.instance()), ConsistencyAnnotatedRecord.serde(GPSSerde.instance())))
                .windowedBy(annotationAwareTimeWindows)
                .reduce(new Reducer<ConsistencyAnnotatedRecord<ValueAndTimestamp<GPS>>>() {
                    @Override
                    public ConsistencyAnnotatedRecord<ValueAndTimestamp<GPS>> apply(ConsistencyAnnotatedRecord<ValueAndTimestamp<GPS>> value1, ConsistencyAnnotatedRecord<ValueAndTimestamp<GPS>> value2) {
                        return value1.withPolynomial(value1.getPolynomial().times(value2.getPolynomial()));
                    }
                }, annotatedRecordWindowStoreMaterialized).toStream().selectKey(new KeyValueMapper<Windowed<ValueAndTimestamp<GPS>>, ConsistencyAnnotatedRecord<ValueAndTimestamp<GPS>>, String>() {
                    @Override
                    public String apply(Windowed<ValueAndTimestamp<GPS>> key, ConsistencyAnnotatedRecord<ValueAndTimestamp<GPS>> value) {
                        return key.key().value().key();
                    }
                }).transform(new TransformerSupplier<String, ConsistencyAnnotatedRecord<ValueAndTimestamp<GPS>>, KeyValue<String, ConsistencyAnnotatedRecord<ValueAndTimestamp<GPS>>>>() {
                    @Override
                    public Transformer<String, ConsistencyAnnotatedRecord<ValueAndTimestamp<GPS>>, KeyValue<String, ConsistencyAnnotatedRecord<ValueAndTimestamp<GPS>>>> get() {
                        return new PerformanceInputInconsistencyTransformer<>(applicationSupplier, props);
                    }
                });



//        annotatedKStream.process(new ProcessorSupplier<String, ConsistencyAnnotatedRecord<ValueAndTimestamp<GPS>>, Void, Void>() {
//            @Override
//            public Processor<String, ConsistencyAnnotatedRecord<ValueAndTimestamp<GPS>>, Void, Void> get() {
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

        annotatedKStream.process(new ProcessorSupplier<String, ConsistencyAnnotatedRecord<ValueAndTimestamp<GPS>>, Void, Void>() {
            @Override
            public Processor<String, ConsistencyAnnotatedRecord<ValueAndTimestamp<GPS>>, Void, Void> get() {
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
