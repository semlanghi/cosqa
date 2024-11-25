import gps.GPS;
import gps.GPSSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import repair.ValueRepairTransformer;
import repair.ValueRepairTransformerKeyBased;
import utils.ApplicationSupplier;
import utils.ExperimentConfig;
import utils.NotAnnotatedPerformanceInputInconsistencyTransformer;

import java.time.Duration;
import java.util.Properties;
import java.util.UUID;

public class DummyRepairGPS {

    public static void main(String[] args) {


        Logger logger = LoggerFactory.getLogger(DummyRepairGPS.class);
        Properties props = new Properties();
        String appID = UUID.randomUUID().toString();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Serdes.String().deserializer().getClass());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GPSSerde.instance().deserializer().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GPSSerde.instance().getClass());
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

        props.put(ExperimentConfig.RESULT_FILE_SUFFIX, "dummy-songrepair-gps");

        int constraintStrictness = Integer.parseInt(props.getProperty(ExperimentConfig.CONSTRAINT_STRICTNESS));
        Duration size = Duration.ofMillis(Long.parseLong(props.getProperty(ExperimentConfig.WINDOW_SIZE_MS)));
        Duration advance = Duration.ofMillis(Long.parseLong(props.getProperty(ExperimentConfig.WINDOW_SLIDE_MS)));
        TimeWindows timeWindows1 = TimeWindows.ofSizeAndGrace(size, size).advanceBy(advance);
        String topic = args[7];


        ApplicationSupplier applicationSupplier = new ApplicationSupplier(1);
        StreamsBuilder builder = new StreamsBuilder();



        builder.stream(topic, Consumed.with(Serdes.String(), GPSSerde.instance(), new TimestampExtractor() {
                            @Override
                            public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
                                return ((GPS)record.value()).timestamp();
                            }
                        }, Topology.AutoOffsetReset.EARLIEST))
                        .transform(new TransformerSupplier<String, GPS, KeyValue<String, GPS>>() {
                            @Override
                            public Transformer<String, GPS, KeyValue<String, GPS>> get() {
                                return new ValueRepairTransformerKeyBased<>(-constraintStrictness, constraintStrictness, timeWindows1, 1);
                            }
                        }).transform(new TransformerSupplier<String, GPS, KeyValue<String, GPS>>() {
                    @Override
                    public Transformer<String, GPS, KeyValue<String, GPS>> get() {
                        return new NotAnnotatedPerformanceInputInconsistencyTransformer<>(applicationSupplier, props);
                    }
                });
//                .process(new ProcessorSupplier<String, GPS, Void, Void>() {
//                    @Override
//                    public Processor<String, GPS, Void, Void> get() {
//                        return new Processor<String, GPS, Void, Void>() {
//                            @Override
//                            public void process(Record<String, GPS> record) {
//                                logger.info(record.value().toString());
//                            }
//                        };
//                    }
//                });




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