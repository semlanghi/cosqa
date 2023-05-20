import annotation.AnnKStream;
import annotation.AnnotationAwareTimeWindows;
import annotation.ConsistencyAnnotatedRecord;
import linearroad.SpeedConstraintLinearRoadValueFactory;
import linearroad.SpeedEvent;
import linearroad.SpeedEventSerde;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.internals.ValueAndTimestampSerde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import topkstreaming.CADistanceBasedRanker;
import topkstreaming.Ranker;
import utils.ApplicationSupplier;
import utils.ExperimentConfig;
import utils.PerformanceProcessorNI;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

public class NILinearRoad {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(NILinearRoad.class);
        Properties props = new Properties();
        String appID = UUID.randomUUID().toString();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Serdes.Long().deserializer().getClass());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SpeedEventSerde.instance().deserializer().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpeedEventSerde.instance().getClass());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
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

        props.put(ExperimentConfig.RESULT_FILE_SUFFIX, "ni-linearroad");

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

        CADistanceBasedRanker ranker = new CADistanceBasedRanker(3, Ranker.Order.DESCENDING);

        StreamsBuilder builder = new StreamsBuilder();
        KStream<Long, SpeedEvent> annotatedKStream = builder
                .stream(topic, Consumed.with(Serdes.Long(), SpeedEventSerde.instance(), new TimestampExtractor() {
                    @Override
                    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
                        return ((SpeedEvent)record.value()).getTimestamp();
                    }
                }, Topology.AutoOffsetReset.EARLIEST));


        ApplicationSupplier applicationSupplier = new ApplicationSupplier(1);

        KStream<Integer, SpeedEvent> windowedSpeedEventAnnKStream = annotatedKStream
                .selectKey(new KeyValueMapper<Long, SpeedEvent, Integer>() {
                    @Override
                    public Integer apply(Long key, SpeedEvent value) {
                        return value.getSegment();
                    }
                });
//                .flatMap(new KeyValueMapper<Long, SpeedEvent, Iterable<KeyValue<Windowed<Integer>, SpeedEvent>>>() {
//            @Override
//            public Iterable<KeyValue<Windowed<Integer>, SpeedEvent>> apply(Long key, SpeedEvent value) {
//                return timeWindows.windowsFor(value.getTimestamp()).entrySet().stream().map(new Function<Map.Entry<Long, TimeWindow>, KeyValue<Windowed<Integer>, SpeedEvent>>() {
//                    @Override
//                    public KeyValue<Windowed<Integer>, SpeedEvent> apply(Map.Entry<Long, TimeWindow> longTimeWindowEntry) {
//                        return new KeyValue<>(new Windowed<>(value.getSegment(), longTimeWindowEntry.getValue()), value);
//                    }
//                }).collect(Collectors.toList());
//            }
//        });



        KStream<Integer, Pair<SpeedEvent, SpeedEvent>> joinedStream = windowedSpeedEventAnnKStream.join(windowedSpeedEventAnnKStream, new ValueJoiner<SpeedEvent, SpeedEvent, Pair<SpeedEvent, SpeedEvent>>() {
                    @Override
                    public Pair<SpeedEvent, SpeedEvent> apply(SpeedEvent value1, SpeedEvent value2) {
                        if (value1.getxWay()!=value2.getxWay())
                            return null;
                        return new ImmutablePair<>(value1, value2);
                    }
                }, joinWindows, StreamJoined.with(Serdes.Integer(), SpeedEventSerde.instance(), SpeedEventSerde.instance())
                    .withThisStoreSupplier(Stores.inMemoryWindowStore("join1"+ UUID.randomUUID(), Duration.ofMillis(joinWindows.size() + joinWindows.gracePeriodMs()), Duration.ofMillis(joinWindows.size()), true))
                    .withOtherStoreSupplier(Stores.inMemoryWindowStore("join2"+UUID.randomUUID(), Duration.ofMillis(joinWindows.size() + joinWindows.gracePeriodMs()), Duration.ofMillis(joinWindows.size()), true)))
                .filter(new Predicate<Integer, Pair<SpeedEvent, SpeedEvent>>() {
                    @Override
                    public boolean test(Integer key, Pair<SpeedEvent, SpeedEvent> value) {
                        return value!=null;
                    }
                });

        joinedStream.process(new ProcessorSupplier<Integer, Pair<SpeedEvent, SpeedEvent>, Void, Void>() {
            @Override
            public Processor<Integer, Pair<SpeedEvent, SpeedEvent>, Void, Void> get() {
                return new PerformanceProcessorNI<>(applicationSupplier, props);
            }
        });

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