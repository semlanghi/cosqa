package electricgrid;

import annotation.AnnKStream;
import annotation.ConsistencyAnnotatedRecord;
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

public class ExampleFilteringOut {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ExampleFilteringOut.class);
        Properties props = new Properties();
        String appID = UUID.randomUUID().toString();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Serdes.Integer().deserializer().getClass());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, EGCSerde.instance().deserializer().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, EGCSerde.instance().getClass());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());

        int constraintStrictness = 2;
        TimeWindows timeWindows = TimeWindows.ofSizeAndGrace(Duration.ofMillis(5), Duration.ofMillis(5)).advanceBy(Duration.ofMillis(2));
        String topic = "dirty-grid2";

        TimeWindows timeWindows1 = TimeWindows.ofSizeAndGrace(Duration.ofMillis(5), Duration.ofMillis(5)).advanceBy(Duration.ofMillis(2));


        ApplicationSupplier applicationSupplier = new ApplicationSupplier(2);

        StreamsBuilder builder = new StreamsBuilder();

        AnnKStream<Integer, EGC> annotatedKStream = AnnKStream.annotateDoubleConstraint(builder
                .stream(topic, Consumed.with(Serdes.Integer(), EGCSerde.instance(), new TimestampExtractor() {
                    @Override
                    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
                        return ((EGC)record.value()).getTs();
                    }
                }, Topology.AutoOffsetReset.EARLIEST)), timeWindows.size(), timeWindows.advanceMs, new SpeedConstraintEGCAValueFactoryWithDescription("SCA", constraintStrictness, -constraintStrictness), new SpeedConstraintEGCBValueFactoryWithDescription("SCB", constraintStrictness, -constraintStrictness), applicationSupplier, props);

        annotatedKStream
                .filterOnAnnotation(new Predicate<Integer, ConsistencyAnnotatedRecord<ValueAndTimestamp<EGC>>>() {
                    @Override
                    public boolean test(Integer key, ConsistencyAnnotatedRecord<ValueAndTimestamp<EGC>> value) {
                        boolean b = value.getPolynomial().getDegree() == 0;
                        return b;
                    }
                })
                .groupByKeyAndWindowBy(timeWindows1, Grouped.with("stock-pair-repartition-" + props.getProperty(StreamsConfig.APPLICATION_ID_CONFIG), Serdes.Integer(), ConsistencyAnnotatedRecord.serde(EGCSerde.instance())))
                .aggregate(new Initializer<AvgEGC>() {
                    @Override
                    public AvgEGC apply() {
                        return new AvgEGC(1, 0, 0, -1);
                    }
                }, new Aggregator<Integer, EGC, AvgEGC>() {
                    @Override
                    public AvgEGC apply(Integer key, EGC value, AvgEGC aggregate) {
                        return new AvgEGC(value.getZone(), value.getConsA() + aggregate.getSumConsA(), value.getConsB() + aggregate.getSumConsB(), Math.max(value.getTs(), aggregate.getTs()));
                    }
                }, Materialized.<Integer, ConsistencyAnnotatedRecord<ValueAndTimestamp<AvgEGC>>>as(Stores.inMemoryWindowStore(appID, Duration.ofMillis(timeWindows1.size() + timeWindows1.gracePeriodMs()), Duration.ofMillis(timeWindows1.size()), false))
                        .withKeySerde(Serdes.Integer()).withValueSerde(ConsistencyAnnotatedRecord.serde(AvgEGCSerde.instance())))
                .getInternalKTable().toStream().process(new ProcessorSupplier<Windowed<Integer>, ConsistencyAnnotatedRecord<ValueAndTimestamp<AvgEGC>>, Void, Void>() {
                    @Override
                    public Processor<Windowed<Integer>, ConsistencyAnnotatedRecord<ValueAndTimestamp<AvgEGC>>, Void, Void> get() {
                        return new Processor<Windowed<Integer>, ConsistencyAnnotatedRecord<ValueAndTimestamp<AvgEGC>>, Void, Void>() {
                            @Override
                            public void process(Record<Windowed<Integer>, ConsistencyAnnotatedRecord<ValueAndTimestamp<AvgEGC>>> record) {
                                System.out.println(record.value().getWrappedRecord().value().ts+","+(record.value().getWrappedRecord().value().getSumConsA()*1.2 + record.value().getWrappedRecord().value().getSumConsB()*1.5));
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