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
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.ApplicationSupplier;
import utils.ExperimentConfig;
import utils.PerformanceProcessor;

import java.time.Duration;
import java.util.Properties;
import java.util.UUID;

public class NCOSQAListLinearRoad {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(NCOSQAListLinearRoad.class);
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

        props.put(ExperimentConfig.RESULT_FILE_SUFFIX, "ncosqa-linearroad-list");

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


        StreamsBuilder builder = new StreamsBuilder();
        AnnKStream<Long, SpeedEvent> annotatedKStream = AnnKStream.annotateListNotWindowed(builder
                .stream(topic, Consumed.with(Serdes.Long(), SpeedEventSerde.instance(), new TimestampExtractor() {
                    @Override
                    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
                        return ((SpeedEvent)record.value()).getTimestamp();
                    }
                }, Topology.AutoOffsetReset.EARLIEST)), timeWindows.size(), timeWindows.advanceMs, new SpeedConstraintLinearRoadValueFactory(0.01/constraintStrictness, -0.01/constraintStrictness));


        ApplicationSupplier applicationSupplier = new ApplicationSupplier(1);

        AnnKStream<Integer, SpeedEvent> windowedSpeedEventAnnKStream = annotatedKStream
                .selectKey(new KeyValueMapper<Long, ValueAndTimestamp<SpeedEvent>, Integer>() {
            @Override
            public Integer apply(Long key, ValueAndTimestamp<SpeedEvent> value) {
                return value.value().getSegment();
            }
        });

        AnnKStream<Integer, Pair<SpeedEvent, SpeedEvent>> joinedStream = windowedSpeedEventAnnKStream.join(windowedSpeedEventAnnKStream, new ValueJoiner<SpeedEvent, SpeedEvent, Pair<SpeedEvent, SpeedEvent>>() {
                    @Override
                    public Pair<SpeedEvent, SpeedEvent> apply(SpeedEvent value1, SpeedEvent value2) {
                        if (value1.getxWay()!=value2.getxWay())
                            return null;
                        return new ImmutablePair<>(value1, value2);
                    }
                },
                joinWindows,
                Serdes.Integer(), SpeedEventSerde.instance()).filterNullValues();


        joinedStream.getInternalKStream().process(new ProcessorSupplier<Integer, ConsistencyAnnotatedRecord<ValueAndTimestamp<Pair<SpeedEvent, SpeedEvent>>>, Void, Void>() {
            @Override
            public Processor<Integer, ConsistencyAnnotatedRecord<ValueAndTimestamp<Pair<SpeedEvent, SpeedEvent>>>, Void, Void> get() {
                return new PerformanceProcessor<>(applicationSupplier, props);
            }
        });

//        Deleted for the sole purpose of the evaluation, otherwise cannot reach event limit, complexity negligible since it is a stateless operation
//        .filterOnAnnotation(new Predicate<Integer, ConsistencyAnnotatedRecord<ValueAndTimestamp<Pair<SpeedEvent, SpeedEvent>>>>() {
//            @Override
//            public boolean test(Integer key, ConsistencyAnnotatedRecord<ValueAndTimestamp<Pair<SpeedEvent, SpeedEvent>>> value) {
//                return value.getPolynomial().getMonomialsDegreeSum() > threshold;
//            }
//        })

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