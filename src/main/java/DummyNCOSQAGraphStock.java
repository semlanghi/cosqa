import annotation.AnnKStream;
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
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stocks.SpeedConstraintStockValueFactory;
import stocks.Stock;
import stocks.StockSerde;
import topkstreaming.CADistanceBasedRanker;
import topkstreaming.InMemoryTopKKeyValueStore;
import topkstreaming.Ranker;
import topkstreaming.TopKCAProcessorNotWindowed;
import utils.ApplicationSupplier;
import utils.ExperimentConfig;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static annotation.AnnWindowedTableImpl.TOP_K_NAME;

public class DummyNCOSQAGraphStock {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(DummyNCOSQAGraphStock.class);
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

        props.put(ExperimentConfig.RESULT_FILE_SUFFIX, "dummy-ncosqa-stock-graph-combo");

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

        ApplicationSupplier applicationSupplier = new ApplicationSupplier(1);

        StreamsBuilder builder = new StreamsBuilder();

        AnnKStream<String, Stock> annotatedKStream = AnnKStream.annotateGraphListNotWindowed(builder
                .stream(topic, Consumed.with(Serdes.String(), StockSerde.instance(), new TimestampExtractor() {
                    @Override
                    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
                        return ((Stock)record.value()).getTs();
                    }
                }, Topology.AutoOffsetReset.EARLIEST)), timeWindows.size(), timeWindows.advanceMs, new SpeedConstraintStockValueFactory(0.1/constraintStrictness, -0.1/constraintStrictness), applicationSupplier, props);



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