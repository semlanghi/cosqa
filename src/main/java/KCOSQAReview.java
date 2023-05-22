import annotation.AnnotationAwareTimeWindows;
import annotation.ConsistencyAnnotatedRecord;
import annotation.constraint.ConstraintFactory;
import annotation.constraint.StreamingConstraint;
import annotation.polynomial.Monomial;
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
import reviews.Review;
import reviews.ReviewSerde;
import reviews.SpeedConstraintReviewValueFactory;
import topkstreaming.*;
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


public class KCOSQAReview {
    public static void main(String[] args){

        Logger logger = LoggerFactory.getLogger(KCOSQAReview.class);
        Properties props = new Properties();
        String appID = UUID.randomUUID().toString();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Serdes.String().deserializer().getClass());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ReviewSerde.ReviewDeserializer.class);
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

        props.put(ExperimentConfig.RESULT_FILE_SUFFIX, "kcosqa-review");

        int constraintStrictness = Integer.parseInt(props.getProperty(ExperimentConfig.CONSTRAINT_STRICTNESS));
        Duration size = Duration.ofMillis(Long.parseLong(props.getProperty(ExperimentConfig.WINDOW_SIZE_MS)));
        Duration advance = Duration.ofMillis(Long.parseLong(props.getProperty(ExperimentConfig.WINDOW_SLIDE_MS)));
        TimeWindows timeWindows = TimeWindows.ofSizeAndGrace(size, size).advanceBy(advance);
        AnnotationAwareTimeWindows annotationAwareTimeWindows = AnnotationAwareTimeWindows.ofSizeAndGrace(size, size)
                .advanceBy(advance);
        JoinWindows joinWindows = JoinWindows.ofTimeDifferenceAndGrace(Duration.ofMillis(timeWindows.size()/2), size)
                .after(Duration.ZERO).before(size);
        String topic = args[7];

        //Building a Review stream within the ValueAndTimestamp Object
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, Review> ReviewKStream = builder
                .stream(topic, Consumed.with(Serdes.String(), ReviewSerde.instance(),new TimestampExtractor() {
                    @Override
                    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
                        return ((Review)record.value()).timestamp();
                    }
                }, Topology.AutoOffsetReset.EARLIEST));


        Materialized<ValueAndTimestamp<Review>, ConsistencyAnnotatedRecord<ValueAndTimestamp<Review>>, KeyValueStore<Bytes, byte[]>> annotatedRecordsState =
                Materialized.as(new InMemoryKeyValueBytesStoreSupplier(ANNOTATE_NAME + "-baseline"));
        annotatedRecordsState.withKeySerde(new ValueAndTimestampSerde<>(ReviewSerde.instance()));
        annotatedRecordsState.withValueSerde(ConsistencyAnnotatedRecord.serde(ReviewSerde.instance()));
        annotatedRecordsState.withCachingDisabled();
        annotatedRecordsState.withLoggingDisabled();


        ConstraintFactory<ValueAndTimestamp<Review>> speedConstraintReviewValueFactory = new SpeedConstraintReviewValueFactory(0.09/constraintStrictness, -0.09/constraintStrictness);
        KStream<String, ValueAndTimestamp<Review>> aggregatedStream = ReviewKStream
                .transformValues(new ValueTransformerSupplier<>() {
                    @Override
                    public ValueTransformer<Review, ValueAndTimestamp<Review>> get() {
                        return new ValueTransformer<>() {
                            ProcessorContext context;

                            @Override
                            public void init(ProcessorContext context) {
                                this.context = context;
                            }

                            @Override
                            public ValueAndTimestamp<Review> transform(Review value) {
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

        Materialized<ValueAndTimestamp<Review>, ConsistencyAnnotatedRecord<ValueAndTimestamp<Review>>, WindowStore<Bytes, byte[]>> annotatedRecordWindowStoreMaterialized = Materialized.
                <ValueAndTimestamp<Review>, ConsistencyAnnotatedRecord<ValueAndTimestamp<Review>>>as(Stores.inMemoryWindowStore("reduce-annot-state-store"+appID, Duration.ofMillis(annotationAwareTimeWindows.size() + annotationAwareTimeWindows.gracePeriodMs()), Duration.ofMillis(annotationAwareTimeWindows.size()), false))
                .withKeySerde(new ValueAndTimestampSerde(ReviewSerde.instance()))
                .withValueSerde(ConsistencyAnnotatedRecord.serde(ReviewSerde.instance()));

        StreamJoined<String, ValueAndTimestamp<Review>, ValueAndTimestamp<Review>> joined = StreamJoined.with(Serdes.String(), new ValueAndTimestampSerde(ReviewSerde.instance()), new ValueAndTimestampSerde(ReviewSerde.instance()))
                .withThisStoreSupplier(Stores.inMemoryWindowStore("join1-annot-" + UUID.randomUUID(), Duration.ofMillis(timeWindows.size() + timeWindows.gracePeriodMs()), Duration.ofMillis(timeWindows.size()), true))
                .withOtherStoreSupplier(Stores.inMemoryWindowStore("join2-annot-" + UUID.randomUUID(), Duration.ofMillis(timeWindows.size() + timeWindows.gracePeriodMs()), Duration.ofMillis(timeWindows.size()), true));
        KStream<String, ConsistencyAnnotatedRecord<ValueAndTimestamp<Review>>> annotatedKStream = aggregatedStream
                .join(aggregatedStream, new ValueJoiner<ValueAndTimestamp<Review>, ValueAndTimestamp<Review>, ConsistencyAnnotatedRecord<ValueAndTimestamp<Review>>>() {
                            @Override
                            public ConsistencyAnnotatedRecord<ValueAndTimestamp<Review>> apply(ValueAndTimestamp<Review> value1, ValueAndTimestamp<Review> value2) {
                                StreamingConstraint<ValueAndTimestamp<Review>> speedConstraint = speedConstraintReviewValueFactory.make(value2);
                                double result = speedConstraint.checkConstraint(value1);

                                ConsistencyAnnotatedRecord<ValueAndTimestamp<Review>> tmp =
                                        new ConsistencyAnnotatedRecord<>(value1);
                                if (result < 0 || result > 0) {
                                    tmp.setPolynomial(tmp.getPolynomial().times(value2, (int) Math.ceil(Math.abs(result))));
                                }
                                return tmp;
                            }
                        }, joinWindows
                        , joined)
                .selectKey(new KeyValueMapper<String, ConsistencyAnnotatedRecord<ValueAndTimestamp<Review>>, ValueAndTimestamp<Review>>() {
                    @Override
                    public ValueAndTimestamp<Review> apply(String key, ConsistencyAnnotatedRecord<ValueAndTimestamp<Review>> value) {
                        return value.getWrappedRecord();
                    }
                })
                .groupByKey(Grouped.with(new ValueAndTimestampSerde<>(ReviewSerde.instance()), ConsistencyAnnotatedRecord.serde(ReviewSerde.instance())))
                .windowedBy(annotationAwareTimeWindows)
                .reduce(new Reducer<ConsistencyAnnotatedRecord<ValueAndTimestamp<Review>>>() {
                    @Override
                    public ConsistencyAnnotatedRecord<ValueAndTimestamp<Review>> apply(ConsistencyAnnotatedRecord<ValueAndTimestamp<Review>> value1, ConsistencyAnnotatedRecord<ValueAndTimestamp<Review>> value2) {
                        return value1.withPolynomial(value1.getPolynomial().times(value2.getPolynomial()));
                    }
                }, annotatedRecordWindowStoreMaterialized)
                .toStream().selectKey(new KeyValueMapper<Windowed<ValueAndTimestamp<Review>>, ConsistencyAnnotatedRecord<ValueAndTimestamp<Review>>, String>() {
                    @Override
                    public String apply(Windowed<ValueAndTimestamp<Review>> key, ConsistencyAnnotatedRecord<ValueAndTimestamp<Review>> value) {
                        return key.key().value().key();
                    }
                });

        ApplicationSupplier applicationSupplier = new ApplicationSupplier(1);


//        annotatedKStream.process(new ProcessorSupplier<String, ConsistencyAnnotatedRecord<ValueAndTimestamp<Review>>, Void, Void>() {
//            @Override
//            public Processor<String, ConsistencyAnnotatedRecord<ValueAndTimestamp<Review>>, Void, Void> get() {
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

        annotatedKStream.process(new ProcessorSupplier<String, ConsistencyAnnotatedRecord<ValueAndTimestamp<Review>>, Void, Void>() {
            @Override
            public Processor<String, ConsistencyAnnotatedRecord<ValueAndTimestamp<Review>>, Void, Void> get() {
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
