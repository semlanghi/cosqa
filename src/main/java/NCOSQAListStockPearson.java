import annotation.*;
import stocks.SpeedConstraintStockValueFactory;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
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
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.ApplicationSupplier;

import java.time.Duration;
import java.util.Properties;
import java.util.UUID;

public class NCOSQAListStockPearson {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(NCOSQAListStockPearson.class);
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, UUID.randomUUID().toString());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Serdes.String().deserializer().getClass());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StockSerde.instance().deserializer().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, StockSerde.instance().getClass());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());


        TimeWindows timeWindows = TimeWindows.ofSizeWithNoGrace(Duration.ofDays(10)).advanceBy(Duration.ofDays(2));

        StreamsBuilder builder = new StreamsBuilder();
        AnnKStream<String, Stock> annotatedKStream = AnnKStream.annotateLinkedMap(builder
                .stream("sample-topic-1", Consumed.with(Serdes.String(), StockSerde.instance(), new TimestampExtractor() {
                    @Override
                    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
                        return ((Stock)record.value()).getTs();
                    }
                }, Topology.AutoOffsetReset.EARLIEST)), timeWindows.size(), timeWindows.advanceMs, new SpeedConstraintStockValueFactory(0.001, -0.001));


        ApplicationSupplier applicationSupplier = new ApplicationSupplier(1);

        //Removed for testing
        AnnKStream<Long, Stock> annotatedTimestampedKStream = annotatedKStream
                .selectKey((key, value) -> value.timestamp());

        AnnKStream<Long , Pair<Stock, Stock>> joinedStream = annotatedTimestampedKStream.join(annotatedTimestampedKStream, new ValueJoiner<Stock, Stock, Pair<Stock, Stock>>() {
                    @Override
                    public Pair<Stock, Stock> apply(Stock value1, Stock value2) {
                        return new ImmutablePair<>(value1, value2);
                    }
                },
                JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofDays(5)).after(Duration.ZERO).before(Duration.ofDays(10)),
                Serdes.Long(), StockSerde.instance());


        AnnWindowedTable<String, PearsonAggregate> diffStream = joinedStream
                .groupAndWindowBy(new KeyValueMapper<Long, Pair<Stock, Stock>, String>() {
                    @Override
                    public String apply(Long key, Pair<Stock, Stock> value) {
                        return value.getLeft().getName().toString()+""+value.getRight().getName().toString();
                    }
                }, timeWindows, Grouped.with("stock-pair-repartition-" + props.getProperty(StreamsConfig.APPLICATION_ID_CONFIG), Serdes.String(), ConsistencyAnnotatedRecord.serde(PairStockSerde.instance())))
                .aggregate((Initializer<PearsonAggregate>) () -> new PearsonAggregate('Z', 'Z'),
                        new Aggregator<String, Pair<Stock, Stock>, PearsonAggregate>() {
                            @Override
                            public PearsonAggregate apply(String key, Pair<Stock, Stock> value, PearsonAggregate aggregate) {
                                return aggregate.addUp(value);
                            }
                        }, Materialized.<String, ConsistencyAnnotatedRecord<ValueAndTimestamp<PearsonAggregate>>>as(Stores.inMemoryWindowStore("cdscd", Duration.ofMillis(timeWindows.size() + timeWindows.gracePeriodMs()), Duration.ofMillis(timeWindows.size()), false))
                                .withKeySerde(Serdes.String()).withValueSerde(ConsistencyAnnotatedRecord.serde(PearsonAggregate.serde())));

//        annotatedKStream.getInternalKStream().process(() -> new ThroughputProcessor<>("ca-ann-list-inconsistent", applicationSupplier));

        diffStream.getInternalKTable().toStream().process(new ProcessorSupplier<Windowed<String>, ConsistencyAnnotatedRecord<ValueAndTimestamp<PearsonAggregate>>, Void, Void>() {
            @Override
            public Processor<Windowed<String>, ConsistencyAnnotatedRecord<ValueAndTimestamp<PearsonAggregate>>, Void, Void> get() {
                return new Processor<Windowed<String>, ConsistencyAnnotatedRecord<ValueAndTimestamp<PearsonAggregate>>, Void, Void>() {
                    @Override
                    public void process(Record<Windowed<String>, ConsistencyAnnotatedRecord<ValueAndTimestamp<PearsonAggregate>>> record) {
                        System.out.println(record);
                    }
                };
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