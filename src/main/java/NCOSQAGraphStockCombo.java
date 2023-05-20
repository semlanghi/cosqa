import annotation.AnnKStream;
import annotation.AnnWindowedTable;
import annotation.AnnotationAwareTimeWindows;
import annotation.ConsistencyAnnotatedRecord;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stocks.*;
import topkstreaming.*;
import utils.ApplicationSupplier;
import utils.ExperimentConfig;
import utils.PerformanceProcessor;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static annotation.AnnWindowedTableImpl.TOP_K_NAME;

public class NCOSQAGraphStockCombo {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(NCOSQAGraphStockCombo.class);
        Properties props = new Properties();
        String appID = UUID.randomUUID().toString();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Serdes.String().deserializer().getClass());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StockSerde.instance().deserializer().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, StockSerde.instance().getClass());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
//        props.put(StreamsConfig.topicPrefix(TopicConfig.RETENTION_MS_CONFIG), Duration.ofDays(10).toMillis());
        props.put(StreamsConfig.topicPrefix(TopicConfig.SEGMENT_BYTES_CONFIG), Integer.MAX_VALUE);
        props.put(StreamsConfig.topicPrefix(TopicConfig.SEGMENT_MS_CONFIG), Long.MAX_VALUE);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());


        props.put(ExperimentConfig.CONSTRAINT_STRICTNESS, args[0]);
        props.put(ExperimentConfig.INCONSISTENCY_PERCENTAGE, args[1]);
        props.put(ExperimentConfig.WINDOW_SIZE_MS, args[2]);
        props.put(ExperimentConfig.WINDOW_SLIDE_MS, args[3]);
        props.put(ExperimentConfig.RESULT_FILE_DIR, args[4]);
        props.put(ExperimentConfig.EVENTS_MAX, args[5]);
        props.put(ExperimentConfig.EVENTS_GRANULARITY, args[6]);

        props.put(ExperimentConfig.RESULT_FILE_SUFFIX, "ncosqa-stock-graph-combo");

        int constraintStrictness = Integer.parseInt(props.getProperty(ExperimentConfig.CONSTRAINT_STRICTNESS));
        Duration size = Duration.ofMillis(Long.parseLong(props.getProperty(ExperimentConfig.WINDOW_SIZE_MS)));
        Duration advance = Duration.ofMillis(Long.parseLong(props.getProperty(ExperimentConfig.WINDOW_SLIDE_MS)));
        TimeWindows timeWindows = TimeWindows.ofSizeAndGrace(size, size).advanceBy(advance);
        AnnotationAwareTimeWindows annotationAwareTimeWindows = AnnotationAwareTimeWindows.ofSizeAndGrace(size, size)
                .advanceBy(advance);
        JoinWindows joinWindows = JoinWindows.ofTimeDifferenceAndGrace(Duration.ofMillis(timeWindows.size()/2), size)
                .after(Duration.ZERO).before(size);
        String topic = args[7];
        CADistanceBasedRanker ranker = new CADistanceBasedRanker(3, Ranker.Order.DESCENDING);

        StreamsBuilder builder = new StreamsBuilder();

        AnnKStream<String, Stock> annotatedKStream = AnnKStream.annotateGraphListNotWindowed(builder
                .stream(topic, Consumed.with(Serdes.String(), StockSerde.instance(), new TimestampExtractor() {
                    @Override
                    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
                        return ((Stock)record.value()).getTs();
                    }
                }, Topology.AutoOffsetReset.EARLIEST)), timeWindows.size(), timeWindows.advanceMs, new SpeedConstraintStockValueFactory(0.0000000001/constraintStrictness, -0.0000000001/constraintStrictness));


        ApplicationSupplier applicationSupplier = new ApplicationSupplier(1);

        AnnKStream<Long, Stock> windowedStockAnnKStream = annotatedKStream
                .selectKey(new KeyValueMapper<String, ValueAndTimestamp<Stock>, Long>() {
            @Override
            public Long apply(String key, ValueAndTimestamp<Stock> value) {
                return value.timestamp();
            }
        });

        AnnKStream<Long, Pair<Stock, Stock>> joinedStream = windowedStockAnnKStream.join(windowedStockAnnKStream, new ValueJoiner<Stock, Stock, Pair<Stock, Stock>>() {
                    @Override
                    public Pair<Stock, Stock> apply(Stock value1, Stock value2) {
                        if (value1.getName().equals(value2.getName()))
                            return null;
                        return new ImmutablePair<>(value1, value2);
                    }
                },
                joinWindows,
                Serdes.Long(), StockSerde.instance()).filterNullValues();
//
//        joinedStream.getInternalKStream().process(new ProcessorSupplier<Long, ConsistencyAnnotatedRecord<ValueAndTimestamp<Pair<Stock, Stock>>>, Void, Void>() {
//            @Override
//            public Processor<Long, ConsistencyAnnotatedRecord<ValueAndTimestamp<Pair<Stock, Stock>>>, Void, Void> get() {
//                return new PerformanceProcessor<>(applicationSupplier, props);
//            }
//        });


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
        joinedStream.getInternalKStream().process(new ProcessorSupplier<Long, ConsistencyAnnotatedRecord<ValueAndTimestamp<Pair<Stock, Stock>>>, Void, Void>() {
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
                    Thread.sleep(6000);
                    Runtime.getRuntime().exit(0);
                } catch (Throwable ex) {
                    Runtime.getRuntime().halt(-1);
                } finally {
                    Runtime.getRuntime().halt(-1);
                }
            }
        });

        applicationSupplier.setApp(streams);

        streams.start();


    }
}