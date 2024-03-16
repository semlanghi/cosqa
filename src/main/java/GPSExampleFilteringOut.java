import annotation.AnnKStream;
import annotation.ConsistencyAnnotatedRecord;
import gps.GPS;
import gps.GPSSerde;
import gps.SpeedConstraintGPSValueFactoryWithDescriptionX;
import gps.SpeedConstraintGPSValueFactoryWithDescriptionY;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.ApplicationSupplier;

import java.time.Duration;
import java.util.Properties;
import java.util.UUID;

public class GPSExampleFilteringOut {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(GPSExampleFilteringOut.class);
        Properties props = new Properties();
        String appID = UUID.randomUUID().toString();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Serdes.String().deserializer().getClass());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GPSSerde.instance().deserializer().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GPSSerde.instance().getClass());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        int constraintStrictness = 10;
        TimeWindows timeWindows = TimeWindows.ofSizeAndGrace(Duration.ofMillis(100000), Duration.ofMillis(100000)).advanceBy(Duration.ofMillis(100000));
        String topic = "gps-dirty2";

        TimeWindows timeWindows1 = TimeWindows.ofSizeAndGrace(Duration.ofMillis(100000), Duration.ofMillis(100000)).advanceBy(Duration.ofMillis(100000));


        ApplicationSupplier applicationSupplier = new ApplicationSupplier(2);

        StreamsBuilder builder = new StreamsBuilder();

        AnnKStream<String, GPS> annotatedKStream = AnnKStream.annotateDoubleConstraint(builder
                .stream(topic, Consumed.with(Serdes.String(), GPSSerde.instance(), new TimestampExtractor() {
                    @Override
                    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
                        return ((GPS)record.value()).timestamp();
                    }
                }, Topology.AutoOffsetReset.EARLIEST)), timeWindows.size(), timeWindows.advanceMs, new SpeedConstraintGPSValueFactoryWithDescriptionX("SCX", constraintStrictness, -constraintStrictness), new SpeedConstraintGPSValueFactoryWithDescriptionY("SCY", constraintStrictness, -constraintStrictness), applicationSupplier, props);

        annotatedKStream
                .filterOnAnnotation(new Predicate<String, ConsistencyAnnotatedRecord<ValueAndTimestamp<GPS>>>() {
                    @Override
                    public boolean test(String key, ConsistencyAnnotatedRecord<ValueAndTimestamp<GPS>> value) {
                        boolean b = value.getPolynomial().getDegree() == 0;
                        return b;
                    }
                })
                .groupByKeyAndWindowBy(timeWindows1, Grouped.with("stock-pair-repartition-" + props.getProperty(StreamsConfig.APPLICATION_ID_CONFIG), Serdes.String(), ConsistencyAnnotatedRecord.serde(GPSSerde.instance()))).reduce(new Reducer<GPS>() {
                    @Override
                    public GPS apply(GPS value1, GPS value2) {

                        GPS stock = new GPS(Math.max(value1.getX(), value2.getX()), Math.max(value1.getY(), value2.getY()), Math.max(value1.timestamp(), value2.timestamp()));
                        logger.info(stock.toString());
                        return stock;
                    }
                }, Materialized.<String, ConsistencyAnnotatedRecord<ValueAndTimestamp<GPS>>>as(Stores.inMemoryWindowStore(appID, Duration.ofMillis(timeWindows1.size() + timeWindows1.gracePeriodMs()), Duration.ofMillis(timeWindows1.size()), false))
                        .withKeySerde(Serdes.String()).withValueSerde(ConsistencyAnnotatedRecord.serde(GPSSerde.instance())))
                .getInternalKTable().toStream().process(new ProcessorSupplier<Windowed<String>, ConsistencyAnnotatedRecord<ValueAndTimestamp<GPS>>, Void, Void>() {
                    @Override
                    public Processor<Windowed<String>, ConsistencyAnnotatedRecord<ValueAndTimestamp<GPS>>, Void, Void> get() {
                        return new Processor<Windowed<String>, ConsistencyAnnotatedRecord<ValueAndTimestamp<GPS>>, Void, Void>() {
                            @Override
                            public void process(Record<Windowed<String>, ConsistencyAnnotatedRecord<ValueAndTimestamp<GPS>>> record) {
                                logger.info(record.toString());
                            }
                        };
                    }
                });



        KafkaStreams streams = new KafkaStreams(builder.build(), props);

//        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
//            Runtime.getRuntime().halt(0);
//        }));
//
//        streams.setStateListener((newState, oldState) -> {
//            if (KafkaStreams.State.PENDING_SHUTDOWN.equals(newState)) {
//                try {
//                    Thread.sleep(6000);
//                    Runtime.getRuntime().exit(0);
//                } catch (Throwable ex) {
//                    Runtime.getRuntime().halt(-1);
//                } finally {
//                    Runtime.getRuntime().halt(-1);
//                }
//            }
//        });
//
//        applicationSupplier.setApp(streams);

        streams.start();


    }
}