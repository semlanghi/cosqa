import annotation.AnnotationAwareTimeWindows;
import annotation.ConsistencyAnnotatedRecord;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import stocks.PairStockSerde;
import stocks.PearsonAggregate;
import stocks.Stock;
import stocks.StockSerde;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import topkstreaming.*;
import utils.ApplicationSupplier;
import utils.ExperimentConfig;
import utils.PerformanceProcessorNI;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static annotation.AnnWindowedTableImpl.TOP_K_NAME;

public class NIStockPearson {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(NIStockPearson.class);
        Properties props = new Properties();
        String appID = UUID.randomUUID().toString();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Serdes.String().deserializer().getClass());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StockSerde.instance().deserializer().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, StockSerde.instance().getClass());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
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

        props.put(ExperimentConfig.RESULT_FILE_SUFFIX, "ni-stock-pearson");

        int constraintStrictness = Integer.parseInt(props.getProperty(ExperimentConfig.CONSTRAINT_STRICTNESS));
        Duration size = Duration.ofMillis(Long.parseLong(props.getProperty(ExperimentConfig.WINDOW_SIZE_MS)));
        Duration advance = Duration.ofMillis(Long.parseLong(props.getProperty(ExperimentConfig.WINDOW_SLIDE_MS)));
        TimeWindows timeWindows = TimeWindows.ofSizeAndGrace(size, size).advanceBy(advance);
        AnnotationAwareTimeWindows annotationAwareTimeWindows = AnnotationAwareTimeWindows.ofSizeAndGrace(size, size)
                .advanceBy(advance);
        JoinWindows joinWindows = JoinWindows.ofTimeDifferenceAndGrace(Duration.ofMillis(timeWindows.size()/2), size)
                .after(Duration.ZERO).before(size);
        String topic = args[7];

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, Stock> annotatedKStream = builder
                .stream(topic, Consumed.with(Serdes.String(), StockSerde.instance(), new TimestampExtractor() {
                    @Override
                    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
                        return ((Stock)record.value()).getTs();
                    }
                }, Topology.AutoOffsetReset.EARLIEST));


        ApplicationSupplier applicationSupplier = new ApplicationSupplier(1);

        //Removed for testing
        KStream<Long, Stock> annotatedTimestampedKStream = annotatedKStream
                .selectKey((key, value) -> value.getTs());

        KStream<Long , Pair<Stock, Stock>> joinedStream = annotatedTimestampedKStream.join(annotatedTimestampedKStream, new ValueJoiner<Stock, Stock, Pair<Stock, Stock>>() {
                    @Override
                    public Pair<Stock, Stock> apply(Stock value1, Stock value2) {
                        if (value1.getName().equals(value2.getName()))
                            return null;
                        return new ImmutablePair<>(value1, value2);
                    }
                },
                joinWindows,
                StreamJoined.with(Serdes.Long(), StockSerde.instance(), StockSerde.instance())
                        .withThisStoreSupplier(Stores.inMemoryWindowStore("join1", Duration.ofMillis(joinWindows.size() + joinWindows.gracePeriodMs()), Duration.ofMillis(joinWindows.size()), true))
                        .withOtherStoreSupplier(Stores.inMemoryWindowStore("join2", Duration.ofMillis(joinWindows.size() + joinWindows.gracePeriodMs()), Duration.ofMillis(joinWindows.size()), true)))
                .filter(new Predicate<Long, Pair<Stock, Stock>>() {
                    @Override
                    public boolean test(Long key, Pair<Stock, Stock> value) {
                        if (value!=null)
                            return true;
                        else return false;
                    }
                });;


        KTable<Windowed<String>, PearsonAggregate> diffStream = joinedStream
                .groupBy(new KeyValueMapper<Long, Pair<Stock, Stock>, String>() {
                    @Override
                    public String apply(Long key, Pair<Stock, Stock> value) {
                        return value.getLeft().getName().toString()+""+value.getRight().getName().toString();
                    }
                }, Grouped.with("stock-pair-repartition-" + props.getProperty(StreamsConfig.APPLICATION_ID_CONFIG), Serdes.String(), PairStockSerde.instance()))
                .windowedBy(timeWindows)
                .aggregate(new Initializer<PearsonAggregate>() {
                    @Override
                    public PearsonAggregate apply() {
                        return new PearsonAggregate('Z', 'Z');
                    }
                }, new Aggregator<String, Pair<Stock, Stock>, PearsonAggregate>() {
                    @Override
                    public PearsonAggregate apply(String key, Pair<Stock, Stock> value, PearsonAggregate aggregate) {
                        return aggregate.addUp(value);
                    }
                }, Materialized.<String, PearsonAggregate>as(Stores.inMemoryWindowStore(appID, Duration.ofMillis(timeWindows.size() + timeWindows.gracePeriodMs()), Duration.ofMillis(timeWindows.size()), false))
                        .withKeySerde(Serdes.String()).withValueSerde(PearsonAggregate.serde()));


//        diffStream.toStream().process(new ProcessorSupplier<Windowed<String>, PearsonAggregate, Void, Void>() {
//            @Override
//            public Processor<Windowed<String>, PearsonAggregate, Void, Void> get() {
//                return new PerformanceProcessorNI<>(applicationSupplier, props);
//            }
//        });



        RandomPearsonAggregateRanker ranker = new RandomPearsonAggregateRanker(3, Ranker.Order.DESCENDING);

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

        diffStream.toStream().process(new ProcessorSupplier<Windowed<String>, PearsonAggregate, Void, Void>() {
            @Override
            public Processor<Windowed<String>, PearsonAggregate, Void, Void> get() {
                return new TopKCAProcessorNI(TOP_K_NAME, applicationSupplier, props);
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