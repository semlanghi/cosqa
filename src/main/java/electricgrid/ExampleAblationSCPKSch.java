package electricgrid;

import annotation.AnnKStream;
import annotation.AnnotationAwareTimeWindows;
import annotation.ConsistencyAnnotatedRecord;
import annotation.constraint.ConstraintFactory;
import annotation.constraint.PrimaryKeyConstraint;
import annotation.polynomial.Monomial;
import annotation.polynomial.Polynomial;
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
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stocks.PairStockSerde;
import stocks.Stock;
import utils.ApplicationSupplier;
import utils.ExperimentConfig;

import java.time.Duration;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Stream;

public class ExampleAblationSCPKSch {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ExampleAblationSCPKSch.class);
        Properties props = new Properties();
        String appID = UUID.randomUUID().toString();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Serdes.Integer().deserializer().getClass());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, EGCSerde.instance().deserializer().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, EGCSerde.instance().getClass());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
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
        //args[8] is the configuratino of the constraints, i.e., "SC,PK,Sch"
        props.put(ExperimentConfig.RESULT_FILE_SUFFIX, "example-ablation-"+args[8]);

        int constraintStrictness = Integer.parseInt(props.getProperty(ExperimentConfig.CONSTRAINT_STRICTNESS));
        Duration size = Duration.ofMillis(Long.parseLong(props.getProperty(ExperimentConfig.WINDOW_SIZE_MS)));
        Duration advance = Duration.ofMillis(Long.parseLong(props.getProperty(ExperimentConfig.WINDOW_SLIDE_MS)));
        TimeWindows timeWindows = TimeWindows.ofSizeAndGrace(size, size).advanceBy(advance);
        AnnotationAwareTimeWindows annotationAwareTimeWindows = AnnotationAwareTimeWindows.ofSizeAndGrace(size, size)
                .advanceBy(advance);
        JoinWindows joinWindows = JoinWindows.ofTimeDifferenceAndGrace(Duration.ofMillis(timeWindows.size()/2), size)
                .after(Duration.ZERO).before(size);
        String topic = args[7];
        ApplicationSupplier applicationSupplier = new ApplicationSupplier(1);

        StreamsBuilder builder = new StreamsBuilder();

        List<ConstraintFactory<ValueAndTimestamp<EGC>>> sca = Arrays.stream(args[8].split(",")).map(new Function<String, ConstraintFactory<ValueAndTimestamp<EGC>>>() {
            @Override
            public ConstraintFactory<ValueAndTimestamp<EGC>> apply(String s) {
                switch (s) {
                    case "SC":
                        return new SpeedConstraintEGCAValueFactoryWithDescription("SCA", constraintStrictness, -constraintStrictness);
                    case "PK":
                        return new PKElectricGridValueFactory();
                    case "Sch":
                        return new SchemaConstraintElectricGridValueFactory();
                }
                return null;
            }
        }).toList();

        ConstraintFactory<ValueAndTimestamp<EGC>>[] array = sca.toArray(new ConstraintFactory[0]);

        AnnKStream<Integer, EGC> annotatedKStream = AnnKStream.annotateMultiConstraintOnlyValues(builder
                .stream(topic, Consumed.with(Serdes.Integer(), EGCSerde.instance(), new TimestampExtractor() {
                    @Override
                    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
                        return ((EGC)record.value()).getTs();
                    }
                }, Topology.AutoOffsetReset.EARLIEST)), timeWindows.size(), timeWindows.advanceMs, applicationSupplier, props, args[8], array, 1);

        TimeWindows timeWindows1 = TimeWindows.ofSizeAndGrace(Duration.ofMillis(100), Duration.ofMillis(100)).advanceBy(Duration.ofMillis(20));

        annotatedKStream
                .groupByKeyAndWindowBy(timeWindows1)//, Grouped.with("electricgrid-pair-repartition-" + props.getProperty(StreamsConfig.APPLICATION_ID_CONFIG), Serdes.Integer(), ConsistencyAnnotatedRecord.serde(EGCSerde.instance())))
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
                        .withKeySerde(Serdes.Integer()).withValueSerde(ConsistencyAnnotatedRecord.serde(AvgEGCSerde.instance())));
//                .toStream().process(new ProcessorSupplier<Windowed<Integer>, AvgEGC, Void, Void>() {
//                    @Override
//                    public Processor<Windowed<Integer>, AvgEGC, Void, Void> get() {
//                        return new Processor<Windowed<Integer>, AvgEGC, Void, Void>() {
//                            @Override
//                            public void process(Record<Windowed<Integer>, AvgEGC> record) {
//                                System.out.println(record.value().ts+","+(record.value().getSumConsA()*1.2 + record.value().getSumConsB()*1.5));
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