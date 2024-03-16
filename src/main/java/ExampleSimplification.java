import annotation.AnnKStream;
import annotation.ConsistencyAnnotatedRecord;
import annotation.polynomial.Monomial;
import annotation.polynomial.Polynomial;
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
import stocks.SpeedConstraintStockValueFactoryWithDescription;
import stocks.Stock;
import stocks.StockSerde;
import utils.ApplicationSupplier;

import java.time.Duration;
import java.util.Comparator;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Function;

public class ExampleSimplification {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ExampleSimplification.class);
        Properties props = new Properties();
        String appID = UUID.randomUUID().toString();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Serdes.String().deserializer().getClass());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StockSerde.instance().deserializer().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, StockSerde.instance().getClass());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        int constraintStrictness = 10;
        TimeWindows timeWindows = TimeWindows.ofSizeAndGrace(Duration.ofMillis(100000), Duration.ofMillis(100000)).advanceBy(Duration.ofMillis(100000));
        String topic = "topic3";

        TimeWindows timeWindows1 = TimeWindows.ofSizeAndGrace(Duration.ofMillis(100000), Duration.ofMillis(100000)).advanceBy(Duration.ofMillis(100000));


        ApplicationSupplier applicationSupplier = new ApplicationSupplier(2);

        StreamsBuilder builder = new StreamsBuilder();

        AnnKStream<String, Stock> annotatedKStream = AnnKStream.annotateDoubleConstraint(builder
                .stream(topic, Consumed.with(Serdes.String(), StockSerde.instance(), new TimestampExtractor() {
                    @Override
                    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
                        return ((Stock)record.value()).getTs();
                    }
                }, Topology.AutoOffsetReset.EARLIEST)), timeWindows.size(), timeWindows.advanceMs, new SpeedConstraintStockValueFactoryWithDescription("SCA", constraintStrictness, -constraintStrictness), new SpeedConstraintStockValueFactoryWithDescription("SCB", constraintStrictness*10, -constraintStrictness*10), applicationSupplier, props);

        annotatedKStream
                .transformAnnotation(new ValueMapper<ConsistencyAnnotatedRecord<ValueAndTimestamp<Stock>>, ConsistencyAnnotatedRecord<ValueAndTimestamp<Stock>>>() {
                    @Override
                    public ConsistencyAnnotatedRecord<ValueAndTimestamp<Stock>> apply(ConsistencyAnnotatedRecord<ValueAndTimestamp<Stock>> value) {
                        Polynomial polynomial = value.getPolynomial();

                        for ( Monomial tmp : polynomial.getMonomials()) {
                            if (tmp.getVariables().size()>=2)
                                for (int i = 1; i < 12; i++) {
                                    tmp.simplify("SCA_"+i, "SCB_"+i);
                                }
                        }
                        return value;
                    }
                })
                .mapValuesOnAnnotation(new ValueMapper<ConsistencyAnnotatedRecord<ValueAndTimestamp<Stock>>, Stock>() {
                    @Override
                    public Stock apply(ConsistencyAnnotatedRecord<ValueAndTimestamp<Stock>> value) {
                        Integer maxExpon = value.getPolynomial().getMonomials().stream()
                                .map(monomial -> (Integer) monomial.getExponents().stream()
                                        .max(Comparator.comparingInt(o -> (Integer) o)).get()).max(Integer::compare).get();
                        double adjustedValue = value.getWrappedRecord().value().getValue() - maxExpon;
                        return new Stock(value.getWrappedRecord().value().getName(), adjustedValue, value.getWrappedRecord().timestamp());
                    }
                })
                .groupByKey(Grouped.with("stock-pair-repartition-" + props.getProperty(StreamsConfig.APPLICATION_ID_CONFIG), Serdes.String(), StockSerde.instance())).reduce(new Reducer<Stock>() {
            @Override
            public Stock apply(Stock value1, Stock value2) {

                Stock stock = new Stock(value1.getName(), value1.getValue() + value2.getValue(), value2.getTs());
                logger.info(stock.toString());
                return stock;
            }
        }, Materialized.<String, Stock>as(Stores.inMemoryKeyValueStore(appID))
                .withKeySerde(Serdes.String()).withValueSerde(StockSerde.instance()))
                .toStream().process(new ProcessorSupplier<String, Stock, Void, Void>() {
                    @Override
                    public Processor<String, Stock, Void, Void> get() {
                        return new Processor<String, Stock, Void, Void>() {
                            @Override
                            public void process(Record<String, Stock> record) {
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