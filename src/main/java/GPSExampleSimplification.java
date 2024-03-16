import annotation.AnnKStream;
import annotation.ConsistencyAnnotatedRecord;
import annotation.polynomial.Monomial;
import annotation.polynomial.Polynomial;
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
import java.util.Comparator;
import java.util.Properties;
import java.util.UUID;

public class GPSExampleSimplification {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(GPSExampleSimplification.class);
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
                .transformAnnotation(new ValueMapper<ConsistencyAnnotatedRecord<ValueAndTimestamp<GPS>>, ConsistencyAnnotatedRecord<ValueAndTimestamp<GPS>>>() {
                    @Override
                    public ConsistencyAnnotatedRecord<ValueAndTimestamp<GPS>> apply(ConsistencyAnnotatedRecord<ValueAndTimestamp<GPS>> value) {
                        Polynomial polynomial = value.getPolynomial();

                        for ( Monomial tmp : polynomial.getMonomials()) {
                            if (tmp.getVariables().size()>=2)
                                for (int i = 1; i < 12; i++) {
                                    tmp.simplify("SCX_"+i, "SCY_"+i);
                                }
                        }
                        return value;
                    }
                })
                .mapValuesOnAnnotation(new ValueMapper<ConsistencyAnnotatedRecord<ValueAndTimestamp<GPS>>, GPS>() {
                    @Override
                    public GPS apply(ConsistencyAnnotatedRecord<ValueAndTimestamp<GPS>> value) {
                        //TODO: aggiustare valore alla song con max exp monomio
                        Integer maxExpon = value.getPolynomial().getMonomials().stream()
                                .map(monomial -> (Integer) monomial.getExponents("SCX").stream()
                                        .max(Comparator.comparingInt(o -> (Integer) o)).get()).max(Comparator.comparingInt(o -> o)).get();
                        double adjustedValueX;
                        if (maxExpon>value.getWrappedRecord().value().getX())
                            adjustedValueX = value.getWrappedRecord().value().getX() - (maxExpon/2.0);
                        else adjustedValueX = value.getWrappedRecord().value().getX() - maxExpon;

                        maxExpon = value.getPolynomial().getMonomials().stream()
                                .map(monomial -> (Integer) monomial.getExponents("SCY").stream()
                                        .max(Comparator.comparingInt(o -> (Integer) o)).get()).max(Comparator.comparingInt(o -> o)).get();
                        double adjustedValueY;
                        if (maxExpon>value.getWrappedRecord().value().getY())
                            adjustedValueY = value.getWrappedRecord().value().getY() - (maxExpon/2.0);
                        else adjustedValueY = value.getWrappedRecord().value().getY() - maxExpon;

                        return new GPS(adjustedValueX, adjustedValueY, value.getWrappedRecord().timestamp());
                    }
                })
                .groupByKey(Grouped.with("stock-pair-repartition-" + props.getProperty(StreamsConfig.APPLICATION_ID_CONFIG), Serdes.String(), GPSSerde.instance())).reduce(new Reducer<GPS>() {
                    @Override
                    public GPS apply(GPS value1, GPS value2) {

                        GPS stock = new GPS(Math.max(value1.getX(), value2.getX()), Math.max(value1.getY(), value2.getY()), Math.max(value1.timestamp(), value2.timestamp()));
                        logger.info(stock.toString());
                        return stock;
                    }
                }, Materialized.<String, GPS>as(Stores.inMemoryKeyValueStore(appID))
                        .withKeySerde(Serdes.String()).withValueSerde(GPSSerde.instance()))
                .toStream().process(new ProcessorSupplier<String, GPS, Void, Void>() {
                    @Override
                    public Processor<String, GPS, Void, Void> get() {
                        return new Processor<String, GPS, Void, Void>() {
                            @Override
                            public void process(Record<String, GPS> record) {
                                logger.info(record.value().toString());
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