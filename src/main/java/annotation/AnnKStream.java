package annotation;

import annotation.constraint.ConstraintFactory;
import annotation.constraint.StreamingConstraint;
import annotation.degreestore.*;
import annotation.polynomial.MonomialImplString;
import annotation.polynomial.Polynomial;
import electricgrid.PKElectricGridValueFactory;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.kstream.internals.KStreamImpl;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import stocks.Stock;
import utils.*;

import java.util.*;

public interface AnnKStream<K, V> {

    String ANNOTATE_NAME = "KSTREAM-ANNOTATE-STORE-"+ UUID.randomUUID();
    String ANNOTATE_NAME2 = "KSTREAM-ANNOTATE-STORE-"+ UUID.randomUUID();
    KStream<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>> getInternalKStream();

    KStream<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>> setInternalKStream(KStream<Windowed<Long>, ConsistencyAnnotatedRecord<ValueAndTimestamp<Pair<Stock, Stock>>>> transform);

    public AnnKStream<K,V> project(ValueMapper<V,V> valueMapper);

    public AnnKStream<K,V> transformAnnotation(ValueMapper<ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>> mapper);

    AnnKStream<K, V> filter(Predicate<K, V> internalPredicate);

    AnnKStream<K, V> filterNullValues();

    <VR> AnnKStream<K, VR> mapValues(ValueMapper<V, VR> valueMapper);

    AnnKStream<K, V> filterOnAnnotation(Predicate<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>> predicate);

    KStream<K, V> mapValuesOnAnnotation(ValueMapper<ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>, V> mapper);

    <VR> AnnKStream<K, VR> join(AnnKStream<K, V> otherStream, ValueJoiner<V, V, VR> internalValueJoiner, JoinWindows joinWindows, Serde<K> keySerde, Serde<V> internalValueSerde);


    <VR> AnnKStream<K, VR> leftJoin(AnnKStream<K, V> otherStream, ValueJoiner<V, V, VR> internalValueJoiner, JoinWindows joinWindows, Serde<K> keySerde,
                                    Serde<V> internalValueSerde);

    <KR> AnnKStream<KR, V> selectKey(KeyValueMapper<K, ValueAndTimestamp<V>, KR> mapper);

    <KR,W extends Window> AnnTimeWindowedKStream<KR, V> groupAndWindowBy(KeyValueMapper<K, V, KR> keySelector, Windows<W> windows, Grouped<KR, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>> grouped);





    public static <K,V> AnnKStream<K,V> annotate(KStream<K, V> stream, long scopeSize, long scopeSlide,
                                                      ConstraintFactory<ValueAndTimestamp<V>> valueAndTimestampConstraintFactory) {
        return new AnnKStreamImpl<>(stream.transform(new TransformerSupplier<K, V, KeyValue<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>>>() {
            @Override
            public Transformer<K, V, KeyValue<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>>> get() {
                return new ConsistencyAnnotatorTransformer<>(ANNOTATE_NAME, scopeSize, scopeSlide);
            }

            public Set<StoreBuilder<?>> stores() {
                return Collections.singleton(new StoreBuilder<>() {
                    @Override
                    public StoreBuilder<StateStore> withCachingEnabled() {
                        return null;
                    }

                    @Override
                    public StoreBuilder<StateStore> withCachingDisabled() {
                        return null;
                    }

                    @Override
                    public StoreBuilder<StateStore> withLoggingEnabled(Map<String, String> config) {
                        return null;
                    }

                    @Override
                    public StoreBuilder<StateStore> withLoggingDisabled() {
                        return null;
                    }

                    @Override
                    public StateStore build() {
                        return new InMemoryWindowDegreeStoreCGraph<>(ANNOTATE_NAME, scopeSize, scopeSlide, valueAndTimestampConstraintFactory);
                    }

                    @Override
                    public Map<String, String> logConfig() {
                        return null;
                    }

                    @Override
                    public boolean loggingEnabled() {
                        return false;
                    }

                    @Override
                    public String name() {
                        return ANNOTATE_NAME;
                    }
                });
            }
        }));
    }

    public static <K,V> AnnKStream<Windowed<K>,V> annotateGraphListNonRedundant(KStream<K, V> stream, long scopeSize, long scopeSlide,
                                                          ConstraintFactory<ValueAndTimestamp<V>> valueAndTimestampConstraintFactory) {
        return new AnnKStreamImpl<>(stream.transform(new TransformerSupplier<K, V, KeyValue<Windowed<K>, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>>>() {
            @Override
            public Transformer<K, V, KeyValue<Windowed<K>, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>>> get() {
                return new ConsistencyAnnotatorTransformerNonRedundant<>(ANNOTATE_NAME, scopeSize, scopeSlide);
            }


            public Set<StoreBuilder<?>> stores() {
                return Collections.singleton(new StoreBuilder<>() {
                    @Override
                    public StoreBuilder<StateStore> withCachingEnabled() {
                        return null;
                    }

                    @Override
                    public StoreBuilder<StateStore> withCachingDisabled() {
                        return null;
                    }

                    @Override
                    public StoreBuilder<StateStore> withLoggingEnabled(Map<String, String> config) {
                        return null;
                    }

                    @Override
                    public StoreBuilder<StateStore> withLoggingDisabled() {
                        return null;
                    }

                    @Override
                    public StateStore build() {
                        return new InMemoryWindowDegreeStoreCGraphList<>(ANNOTATE_NAME, scopeSize, scopeSlide, valueAndTimestampConstraintFactory);
                    }

                    @Override
                    public Map<String, String> logConfig() {
                        return null;
                    }

                    @Override
                    public boolean loggingEnabled() {
                        return false;
                    }

                    @Override
                    public String name() {
                        return ANNOTATE_NAME;
                    }
                });
            }
        }));
    }

    public static <K,V> AnnKStream<K,V> annotateGraphListNotWindowed(KStream<K, V> stream, long scopeSize, long scopeSlide,
                                                                     ConstraintFactory<ValueAndTimestamp<V>> valueAndTimestampConstraintFactory, ApplicationSupplier applicationSupplier, Properties props) {
        return new AnnKStreamImpl<>(stream.transform(new TransformerSupplier<K, V, KeyValue<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>>>() {
            @Override
            public Transformer<K, V, KeyValue<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>>> get() {
                return new ConsistencyAnnotatorTransformerNotWindowed<>(ANNOTATE_NAME, scopeSize, scopeSlide);
            }


            public Set<StoreBuilder<?>> stores() {
                return Collections.singleton(new StoreBuilder<>() {
                    @Override
                    public StoreBuilder<StateStore> withCachingEnabled() {
                        return null;
                    }

                    @Override
                    public StoreBuilder<StateStore> withCachingDisabled() {
                        return null;
                    }

                    @Override
                    public StoreBuilder<StateStore> withLoggingEnabled(Map<String, String> config) {
                        return null;
                    }

                    @Override
                    public StoreBuilder<StateStore> withLoggingDisabled() {
                        return null;
                    }

                    @Override
                    public StateStore build() {
                        return new InMemoryWindowDegreeStoreCGraphList<>(ANNOTATE_NAME, scopeSize, scopeSlide, valueAndTimestampConstraintFactory);
                    }

                    @Override
                    public Map<String, String> logConfig() {
                        return null;
                    }

                    @Override
                    public boolean loggingEnabled() {
                        return false;
                    }

                    @Override
                    public String name() {
                        return ANNOTATE_NAME;
                    }
                });
            }
        }).transform(new TransformerSupplier<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>, KeyValue<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>>>() {
            @Override
            public Transformer<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>, KeyValue<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>>> get() {
                return new PerformanceInputInconsistencyTransformer<>(applicationSupplier, props);
            }
        }));
    }


    public static <K,V> KStream<K,V> annotateGraphListFilter(KStream<K, V> stream, long scopeSize, long scopeSlide,
                                                                     ConstraintFactory<ValueAndTimestamp<V>> valueAndTimestampConstraintFactory, ApplicationSupplier applicationSupplier, Properties props) {
        return stream.transform(new TransformerSupplier<K, V, KeyValue<K, V>>() {
            @Override
            public Transformer<K, V, KeyValue<K, V>> get() {
                return new ConsistencyAnnotatorTransformerFilter<>(ANNOTATE_NAME, scopeSize, scopeSlide);
            }


            public Set<StoreBuilder<?>> stores() {
                return Collections.singleton(new StoreBuilder<>() {
                    @Override
                    public StoreBuilder<StateStore> withCachingEnabled() {
                        return null;
                    }

                    @Override
                    public StoreBuilder<StateStore> withCachingDisabled() {
                        return null;
                    }

                    @Override
                    public StoreBuilder<StateStore> withLoggingEnabled(Map<String, String> config) {
                        return null;
                    }

                    @Override
                    public StoreBuilder<StateStore> withLoggingDisabled() {
                        return null;
                    }

                    @Override
                    public StateStore build() {
                        return new InMemoryWindowDegreeStoreCGraphList<>(ANNOTATE_NAME, scopeSize, scopeSlide, valueAndTimestampConstraintFactory);
                    }

                    @Override
                    public Map<String, String> logConfig() {
                        return null;
                    }

                    @Override
                    public boolean loggingEnabled() {
                        return false;
                    }

                    @Override
                    public String name() {
                        return ANNOTATE_NAME;
                    }
                });
            }
        }).transform(new TransformerSupplier<K, V, KeyValue<K, V>>() {
            @Override
            public Transformer<K,V, KeyValue<K, V>> get() {
                return new NotAnnotatedPerformanceInputInconsistencyTransformer<>(applicationSupplier, props);
            }
        });
    }

    public static <K,V> AnnKStream<K,V> annotateIncGraphListNotWindowed(KStream<K, V> stream, long scopeSize, long scopeSlide,
                                                                     ConstraintFactory<ValueAndTimestamp<V>> valueAndTimestampConstraintFactory, ApplicationSupplier applicationSupplier, Properties props) {
        return new AnnKStreamImpl<>(stream.transform(new TransformerSupplier<K, V, KeyValue<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>>>() {
            @Override
            public Transformer<K, V, KeyValue<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>>> get() {
                return new ConsistencyAnnotatorTransformerNotWindowed<>(ANNOTATE_NAME, scopeSize, scopeSlide);
            }


            public Set<StoreBuilder<?>> stores() {
                return Collections.singleton(new StoreBuilder<>() {
                    @Override
                    public StoreBuilder<StateStore> withCachingEnabled() {
                        return null;
                    }

                    @Override
                    public StoreBuilder<StateStore> withCachingDisabled() {
                        return null;
                    }

                    @Override
                    public StoreBuilder<StateStore> withLoggingEnabled(Map<String, String> config) {
                        return null;
                    }

                    @Override
                    public StoreBuilder<StateStore> withLoggingDisabled() {
                        return null;
                    }

                    @Override
                    public StateStore build() {
                        return new InMemoryWindowDegreeStoreIncGraphList<>(ANNOTATE_NAME, scopeSize, scopeSlide, valueAndTimestampConstraintFactory);
                    }

                    @Override
                    public Map<String, String> logConfig() {
                        return null;
                    }

                    @Override
                    public boolean loggingEnabled() {
                        return false;
                    }

                    @Override
                    public String name() {
                        return ANNOTATE_NAME;
                    }
                });
            }
        }).transform(new TransformerSupplier<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>, KeyValue<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>>>() {
            @Override
            public Transformer<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>, KeyValue<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>>> get() {
                return new PerformanceInputInconsistencyTransformer<>(applicationSupplier, props);
            }
        }));
    }

    public static <K,V> AnnKStream<K,V> annotateMultiConstraint(KStream<K, V> stream, long scopeSize, long scopeSlide,
                                                                  ApplicationSupplier applicationSupplier, Properties props, String factoryNames, ConstraintFactory<ValueAndTimestamp<V>>[] valueAndTimestampConstraintFactories) {

        if (factoryNames.equals(""))
            throw new IllegalArgumentException("The number of names must be equal to the number of factories");
        String[] names = factoryNames.split(",");
        if (names.length != valueAndTimestampConstraintFactories.length) {
            throw new IllegalArgumentException("The number of names must be equal to the number of factories");
        }


        String name = "KSTREAM-MULTI-ANNOTATE-STORE-"+ UUID.randomUUID();


        return new AnnKStreamImpl<>(stream.transform(new TransformerSupplier<K, V, KeyValue<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>>>() {
            @Override
            public Transformer<K, V, KeyValue<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>>> get() {
                switch (factoryNames) {
                    case "SC,PK,Sch":
                        return new ConsistencyAnnotatorMultiConstraint<>(scopeSize, scopeSlide, valueAndTimestampConstraintFactories[2], name);
                    case "SC,PK":
                        return new ConsistencyAnnotatorMultiConstraint<>(scopeSize, scopeSlide, name);
                    case "SC,Sch":
                        return new ConsistencyAnnotatorMultiConstraint<>(scopeSize, scopeSlide, valueAndTimestampConstraintFactories[1], name);
                    case "PK,Sch":
                        return new ConsistencyAnnotatorMultiConstraint<>(scopeSize, scopeSlide, valueAndTimestampConstraintFactories[1], name);
                    case "Sch":
                        return new ConsistencyAnnotatorMultiConstraint<>(scopeSize, scopeSlide, valueAndTimestampConstraintFactories[0]);
                    case "SC":
                        return new ConsistencyAnnotatorMultiConstraint<>(scopeSize, scopeSlide, name);
                    case "PK":
                        return new ConsistencyAnnotatorMultiConstraint<>(scopeSize, scopeSlide, name);
                    default: throw new IllegalArgumentException("The factory names are not correct");
                }
            }


            public Set<StoreBuilder<?>> stores() {

                if (factoryNames.equals("Sch"))
                    return null;

                Set<StoreBuilder<?>> storeBuilders = new HashSet<>();

                storeBuilders.add(new StoreBuilder<>() {
                    @Override
                    public StoreBuilder<StateStore> withCachingEnabled() {
                        return null;
                    }

                    @Override
                    public StoreBuilder<StateStore> withCachingDisabled() {
                        return null;
                    }

                    @Override
                    public StoreBuilder<StateStore> withLoggingEnabled(Map<String, String> config) {
                        return null;
                    }

                    @Override
                    public StoreBuilder<StateStore> withLoggingDisabled() {
                        return null;
                    }

                    @Override
                    public StateStore build() {
                        if (factoryNames.contains("SC,PK"))
                            return new InMemoryWindowDegreeMultiStoreCGraphList<>(name, scopeSize, scopeSlide, valueAndTimestampConstraintFactories[0], valueAndTimestampConstraintFactories[1]);
                        if (factoryNames.contains("SC"))
                            return new InMemoryWindowDegreeMultiStoreCGraphList<>(name, scopeSize, scopeSlide, valueAndTimestampConstraintFactories[0], "");
                        else if (factoryNames.contains("PK"))
                            return new InMemoryWindowDegreeMultiStoreCGraphList<>(name, scopeSize, scopeSlide, valueAndTimestampConstraintFactories[0]);
                        else
                            throw new IllegalArgumentException("The factory names are not correct, should not build a store.");
                    }

                    @Override
                    public Map<String, String> logConfig() {
                        return null;
                    }

                    @Override
                    public boolean loggingEnabled() {
                        return false;
                    }

                    @Override
                    public String name() {
                        return name;
                    }
                });



                return storeBuilders;
            }
        }).transform(new TransformerSupplier<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>, KeyValue<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>>>() {
            @Override
            public Transformer<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>, KeyValue<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>>> get() {
                return new PerformanceInputInconsistencyTransformer<>(applicationSupplier, props);
            }
        }));
    }


    public static <K,V> AnnKStream<K,V> annotateMultiConstraintOnlyValues(KStream<K, V> stream, long scopeSize, long scopeSlide,
                                                                ApplicationSupplier applicationSupplier, Properties props, String factoryNames, ConstraintFactory<ValueAndTimestamp<V>>[] valueAndTimestampConstraintFactories, K defaultKey) {

        if (factoryNames.equals(""))
            throw new IllegalArgumentException("The number of names must be equal to the number of factories");
        String[] names = factoryNames.split(",");
        if (names.length != valueAndTimestampConstraintFactories.length) {
            throw new IllegalArgumentException("The number of names must be equal to the number of factories");
        }


        String name = "KSTREAM-MULTI-ANNOTATE-STORE-"+ UUID.randomUUID();


        return new AnnKStreamImpl<>(stream.transformValues(new ValueTransformerSupplier<V, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>>() {
            @Override
            public ValueTransformer<V, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>> get() {
                switch (factoryNames) {
                    case "SC,PK,Sch":
                        return new ConsistencyAnnotatorMultiConstraintOnlyValues<>(scopeSize, scopeSlide, valueAndTimestampConstraintFactories[2], name, defaultKey);
                    case "SC,PK":
                        return new ConsistencyAnnotatorMultiConstraintOnlyValues<>(scopeSize, scopeSlide, name, defaultKey);
                    case "SC,Sch":
                        return new ConsistencyAnnotatorMultiConstraintOnlyValues<>(scopeSize, scopeSlide, valueAndTimestampConstraintFactories[1], name, defaultKey);
                    case "PK,Sch":
                        return new ConsistencyAnnotatorMultiConstraintOnlyValues<>(scopeSize, scopeSlide, valueAndTimestampConstraintFactories[1], name, defaultKey);
                    case "Sch":
                        return new ConsistencyAnnotatorMultiConstraintOnlyValues<>(scopeSize, scopeSlide, valueAndTimestampConstraintFactories[0], defaultKey);
                    case "SC":
                        return new ConsistencyAnnotatorMultiConstraintOnlyValues<>(scopeSize, scopeSlide, name, defaultKey);
                    case "PK":
                        return new ConsistencyAnnotatorMultiConstraintOnlyValues<>(scopeSize, scopeSlide, name, defaultKey);
                    default: throw new IllegalArgumentException("The factory names are not correct");
                }
            }


            public Set<StoreBuilder<?>> stores() {

                if (factoryNames.equals("Sch"))
                    return null;

                Set<StoreBuilder<?>> storeBuilders = new HashSet<>();

                storeBuilders.add(new StoreBuilder<>() {
                    @Override
                    public StoreBuilder<StateStore> withCachingEnabled() {
                        return null;
                    }

                    @Override
                    public StoreBuilder<StateStore> withCachingDisabled() {
                        return null;
                    }

                    @Override
                    public StoreBuilder<StateStore> withLoggingEnabled(Map<String, String> config) {
                        return null;
                    }

                    @Override
                    public StoreBuilder<StateStore> withLoggingDisabled() {
                        return null;
                    }

                    @Override
                    public StateStore build() {
                        if (factoryNames.contains("SC,PK"))
                            return new InMemoryWindowDegreeMultiStoreCGraphList<>(name, scopeSize, scopeSlide, valueAndTimestampConstraintFactories[0], valueAndTimestampConstraintFactories[1]);
                        if (factoryNames.contains("SC"))
                            return new InMemoryWindowDegreeMultiStoreCGraphList<>(name, scopeSize, scopeSlide, valueAndTimestampConstraintFactories[0], "");
                        else if (factoryNames.contains("PK"))
                            return new InMemoryWindowDegreeMultiStoreCGraphList<>(name, scopeSize, scopeSlide, valueAndTimestampConstraintFactories[0]);
                        else
                            throw new IllegalArgumentException("The factory names are not correct, should not build a store.");
                    }

                    @Override
                    public Map<String, String> logConfig() {
                        return null;
                    }

                    @Override
                    public boolean loggingEnabled() {
                        return false;
                    }

                    @Override
                    public String name() {
                        return name;
                    }
                });



                return storeBuilders;
            }
        }).transformValues(new ValueTransformerSupplier<ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>>() {
            @Override
            public ValueTransformer<ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>> get() {
                return new PerformanceInputInconsistencyTransformerOnlyValues<>(applicationSupplier, props);
            }
        }));
    }

    public static <K,V> AnnKStream<K,V> annotateDoubleConstraint(KStream<K, V> stream, long scopeSize, long scopeSlide,
                                                                     ConstraintFactory<ValueAndTimestamp<V>> valueAndTimestampConstraintFactory, ConstraintFactory<ValueAndTimestamp<V>> valueAndTimestampConstraintFactory2, ApplicationSupplier applicationSupplier, Properties props) {
        String ANNOTATE_NAME = "KSTREAM-ANNOTATE-STORE-"+ UUID.randomUUID();
        String ANNOTATE_NAME2 = "KSTREAM-ANNOTATE-STORE-"+ UUID.randomUUID();
        return new AnnKStreamImpl<>(stream.transform(new TransformerSupplier<K, V, KeyValue<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>>>() {
            @Override
            public Transformer<K, V, KeyValue<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>>> get() {
                return new ConsistencyAnnotatorDoubleConstraintSong<>(ANNOTATE_NAME, ANNOTATE_NAME2, scopeSize, scopeSlide);
            }


            public Set<StoreBuilder<?>> stores() {

                StoreBuilder<StateStore> o = new StoreBuilder<>() {
                    @Override
                    public StoreBuilder<StateStore> withCachingEnabled() {
                        return null;
                    }

                    @Override
                    public StoreBuilder<StateStore> withCachingDisabled() {
                        return null;
                    }

                    @Override
                    public StoreBuilder<StateStore> withLoggingEnabled(Map<String, String> config) {
                        return null;
                    }

                    @Override
                    public StoreBuilder<StateStore> withLoggingDisabled() {
                        return null;
                    }

                    @Override
                    public StateStore build() {
                        return new InMemoryWindowDegreeStoreCGraphList<>(ANNOTATE_NAME, scopeSize, scopeSlide, valueAndTimestampConstraintFactory);
                    }

                    @Override
                    public Map<String, String> logConfig() {
                        return null;
                    }

                    @Override
                    public boolean loggingEnabled() {
                        return false;
                    }

                    @Override
                    public String name() {
                        return ANNOTATE_NAME;
                    }
                };

                StoreBuilder<StateStore> o2 = new StoreBuilder<>() {
                    @Override
                    public StoreBuilder<StateStore> withCachingEnabled() {
                        return null;
                    }

                    @Override
                    public StoreBuilder<StateStore> withCachingDisabled() {
                        return null;
                    }

                    @Override
                    public StoreBuilder<StateStore> withLoggingEnabled(Map<String, String> config) {
                        return null;
                    }

                    @Override
                    public StoreBuilder<StateStore> withLoggingDisabled() {
                        return null;
                    }

                    @Override
                    public StateStore build() {
                        return new InMemoryWindowDegreeStoreCGraphList<>(ANNOTATE_NAME2, scopeSize, scopeSlide, valueAndTimestampConstraintFactory2);
                    }

                    @Override
                    public Map<String, String> logConfig() {
                        return null;
                    }

                    @Override
                    public boolean loggingEnabled() {
                        return false;
                    }

                    @Override
                    public String name() {
                        return ANNOTATE_NAME2;
                    }
                };


                return new HashSet<>(Arrays.asList(o,o2));
            }
        }));
    }


    public static <K,V> AnnKStream<K,V> annotateSchemaNotWindowed(KStream<K, V> stream,
                                                                     ConstraintFactory<ValueAndTimestamp<V>> valueAndTimestampConstraintFactory, ApplicationSupplier applicationSupplier, Properties props) {
        return new AnnKStreamImpl<>(stream.transform(new TransformerSupplier<K, V, KeyValue<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>>>() {
            @Override
            public Transformer<K, V, KeyValue<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>>> get() {
                return new Transformer<K, V, KeyValue<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>>>() {

                    final ConstraintFactory<ValueAndTimestamp<V>> constraintFactory = valueAndTimestampConstraintFactory;
                    ProcessorContext context;

                    @Override
                    public void init(ProcessorContext context) {
                        this.context = context;
                    }

                    @Override
                    public KeyValue<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>> transform(K key, V value) {
                        ValueAndTimestamp<V> origin = ValueAndTimestamp.make(value, context.timestamp());
                        StreamingConstraint<ValueAndTimestamp<V>> constraint = valueAndTimestampConstraintFactory.make(origin);
                        // COnstraint true if >0
                        ConsistencyAnnotatedRecord<ValueAndTimestamp<V>> polynomialConsistencyAnnotatedRecord;
                        if (constraint.checkConstraint(origin)<0) {
                            polynomialConsistencyAnnotatedRecord = new ConsistencyAnnotatedRecord<>(new Polynomial(new MonomialImplString(constraint.getDescription(), 1)), origin);
                        } else{
                            polynomialConsistencyAnnotatedRecord = new ConsistencyAnnotatedRecord<>(new Polynomial(), origin);
                        }
                        return new KeyValue<>(key, polynomialConsistencyAnnotatedRecord);
                    }

                    @Override
                    public void close() {

                    }
                };
            }

        }).transform(new TransformerSupplier<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>, KeyValue<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>>>() {
            @Override
            public Transformer<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>, KeyValue<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>>> get() {
                return new PerformanceInputInconsistencyTransformer<>(applicationSupplier, props);
            }
        }));
    }

    public static <K,V> AnnKStream<K,V> annotateGraphListNotWindowedNotAlways(KStream<K, V> stream, long scopeSize, long scopeSlide,
                                                                     ConstraintFactory<ValueAndTimestamp<V>> valueAndTimestampConstraintFactory, ApplicationSupplier applicationSupplier, Properties props) {
        return new AnnKStreamImpl<>(stream.transform(new TransformerSupplier<K, V, KeyValue<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>>>() {
            @Override
            public Transformer<K, V, KeyValue<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>>> get() {
                return new ConsistencyAnnotatorTransformerNotWindowedNotAlways<>(ANNOTATE_NAME, scopeSize, scopeSlide, Integer.parseInt(props.getProperty(ExperimentConfig.INCONSISTENCY_PERCENTAGE)));
            }


            public Set<StoreBuilder<?>> stores() {
                return Collections.singleton(new StoreBuilder<>() {
                    @Override
                    public StoreBuilder<StateStore> withCachingEnabled() {
                        return null;
                    }

                    @Override
                    public StoreBuilder<StateStore> withCachingDisabled() {
                        return null;
                    }

                    @Override
                    public StoreBuilder<StateStore> withLoggingEnabled(Map<String, String> config) {
                        return null;
                    }

                    @Override
                    public StoreBuilder<StateStore> withLoggingDisabled() {
                        return null;
                    }

                    @Override
                    public StateStore build() {
                        return new InMemoryWindowDegreeStoreCGraphList<>(ANNOTATE_NAME, scopeSize, scopeSlide, valueAndTimestampConstraintFactory);
                    }

                    @Override
                    public Map<String, String> logConfig() {
                        return null;
                    }

                    @Override
                    public boolean loggingEnabled() {
                        return false;
                    }

                    @Override
                    public String name() {
                        return ANNOTATE_NAME;
                    }
                });
            }
        }).transform(new TransformerSupplier<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>, KeyValue<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>>>() {
            @Override
            public Transformer<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>, KeyValue<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>>> get() {
                return new PerformanceInputInconsistencyTransformer<>(applicationSupplier, props);
            }
        }));
    }

    public static <K,V> AnnKStream<K,V> annotateDummyNotWindowed(KStream<K, V> stream, long scopeSize, long scopeSlide,
                                                                     ConstraintFactory<ValueAndTimestamp<V>> valueAndTimestampConstraintFactory, ApplicationSupplier applicationSupplier, Properties props) {
        return new AnnKStreamImpl<>(stream.transform(new TransformerSupplier<K, V, KeyValue<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>>>() {
            @Override
            public Transformer<K, V, KeyValue<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>>> get() {
                return new Transformer<K, V, KeyValue<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>>>() {
                    ProcessorContext context;
                    @Override
                    public void init(ProcessorContext context) {
                        this.context = context;
                    }

                    @Override
                    public KeyValue<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>> transform(K key, V value) {
                        return new KeyValue<>(key, new ConsistencyAnnotatedRecord<>(ValueAndTimestamp.make(value, context.timestamp())));
                    }

                    @Override
                    public void close() {

                    }
                };
            }
        }).transform(new TransformerSupplier<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>, KeyValue<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>>>() {
            @Override
            public Transformer<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>, KeyValue<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>>> get() {
                return new PerformanceInputInconsistencyTransformerDummy<>(applicationSupplier, props);
            }
        }));
    }

    public static <K,V> AnnKStream<K,V> annotateListNotWindowed(KStream<K, V> stream, long scopeSize, long scopeSlide,
                                                                ConstraintFactory<ValueAndTimestamp<V>> valueAndTimestampConstraintFactory, ApplicationSupplier applicationSupplier, Properties props) {
        return new AnnKStreamImpl<>(stream.transform(new TransformerSupplier<K, V, KeyValue<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>>>() {
            @Override
            public Transformer<K, V, KeyValue<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>>> get() {
                return new ConsistencyAnnotatorTransformerNotWindowed<>(ANNOTATE_NAME, scopeSize, scopeSlide);
            }


            public Set<StoreBuilder<?>> stores() {
                return Collections.singleton(new StoreBuilder<>() {
                    @Override
                    public StoreBuilder<StateStore> withCachingEnabled() {
                        return null;
                    }

                    @Override
                    public StoreBuilder<StateStore> withCachingDisabled() {
                        return null;
                    }

                    @Override
                    public StoreBuilder<StateStore> withLoggingEnabled(Map<String, String> config) {
                        return null;
                    }

                    @Override
                    public StoreBuilder<StateStore> withLoggingDisabled() {
                        return null;
                    }

                    @Override
                    public StateStore build() {
                        return new InMemoryWindowDegreeStoreLinkedHashMap<>(ANNOTATE_NAME, scopeSize, scopeSlide, valueAndTimestampConstraintFactory);
                    }

                    @Override
                    public Map<String, String> logConfig() {
                        return null;
                    }

                    @Override
                    public boolean loggingEnabled() {
                        return false;
                    }

                    @Override
                    public String name() {
                        return ANNOTATE_NAME;
                    }
                });
            }
        }).transform(new TransformerSupplier<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>, KeyValue<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>>>() {
            @Override
            public Transformer<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>, KeyValue<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>>> get() {
                return new PerformanceInputInconsistencyTransformer<>(applicationSupplier, props);
            }
        }));
    }

    public static <K,V> AnnKStream<K,V> annotateListNotWindowedNotAlways(KStream<K, V> stream, long scopeSize, long scopeSlide,
                                                                ConstraintFactory<ValueAndTimestamp<V>> valueAndTimestampConstraintFactory, ApplicationSupplier applicationSupplier, Properties props) {
        return new AnnKStreamImpl<>(stream.transform(new TransformerSupplier<K, V, KeyValue<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>>>() {
            @Override
            public Transformer<K, V, KeyValue<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>>> get() {
                return new ConsistencyAnnotatorTransformerNotWindowedNotAlways<>(ANNOTATE_NAME, scopeSize, scopeSlide, Integer.parseInt(props.getProperty(ExperimentConfig.INCONSISTENCY_PERCENTAGE)));
            }


            public Set<StoreBuilder<?>> stores() {
                return Collections.singleton(new StoreBuilder<>() {
                    @Override
                    public StoreBuilder<StateStore> withCachingEnabled() {
                        return null;
                    }

                    @Override
                    public StoreBuilder<StateStore> withCachingDisabled() {
                        return null;
                    }

                    @Override
                    public StoreBuilder<StateStore> withLoggingEnabled(Map<String, String> config) {
                        return null;
                    }

                    @Override
                    public StoreBuilder<StateStore> withLoggingDisabled() {
                        return null;
                    }

                    @Override
                    public StateStore build() {
                        return new InMemoryWindowDegreeStoreLinkedHashMap<>(ANNOTATE_NAME, scopeSize, scopeSlide, valueAndTimestampConstraintFactory);
                    }

                    @Override
                    public Map<String, String> logConfig() {
                        return null;
                    }

                    @Override
                    public boolean loggingEnabled() {
                        return false;
                    }

                    @Override
                    public String name() {
                        return ANNOTATE_NAME;
                    }
                });
            }
        }).transform(new TransformerSupplier<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>, KeyValue<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>>>() {
            @Override
            public Transformer<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>, KeyValue<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>>> get() {
                return new PerformanceInputInconsistencyTransformer<>(applicationSupplier, props);
            }
        }));
    }


    public static <K,V> AnnKStream<K,V> annotateGraphList(KStream<K, V> stream, long scopeSize, long scopeSlide,
                                                          ConstraintFactory<ValueAndTimestamp<V>> valueAndTimestampConstraintFactory) {
        return new AnnKStreamImpl<>(stream.transform(new TransformerSupplier<K, V, KeyValue<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>>>() {
            @Override
            public Transformer<K, V, KeyValue<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>>> get() {
                return new ConsistencyAnnotatorTransformer<>(ANNOTATE_NAME, scopeSize, scopeSlide);
            }


            public Set<StoreBuilder<?>> stores() {
                return Collections.singleton(new StoreBuilder<>() {
                    @Override
                    public StoreBuilder<StateStore> withCachingEnabled() {
                        return null;
                    }

                    @Override
                    public StoreBuilder<StateStore> withCachingDisabled() {
                        return null;
                    }

                    @Override
                    public StoreBuilder<StateStore> withLoggingEnabled(Map<String, String> config) {
                        return null;
                    }

                    @Override
                    public StoreBuilder<StateStore> withLoggingDisabled() {
                        return null;
                    }

                    @Override
                    public StateStore build() {
                        return new InMemoryWindowDegreeStoreCGraphList<>(ANNOTATE_NAME, scopeSize, scopeSlide, valueAndTimestampConstraintFactory);
                    }

                    @Override
                    public Map<String, String> logConfig() {
                        return null;
                    }

                    @Override
                    public boolean loggingEnabled() {
                        return false;
                    }

                    @Override
                    public String name() {
                        return ANNOTATE_NAME;
                    }
                });
            }
        }));
    }


    public static <K,V> AnnKStream<K,V> annotateLinkedMap(KStream<K, V> stream, long scopeSize, long scopeSlide,
                                                 ConstraintFactory<ValueAndTimestamp<V>> valueAndTimestampConstraintFactory) {
        return new AnnKStreamImpl<>(stream.transform(new TransformerSupplier<K, V, KeyValue<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>>>() {
            @Override
            public Transformer<K, V, KeyValue<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>>> get() {
                return new ConsistencyAnnotatorTransformer<>(ANNOTATE_NAME, scopeSize, scopeSlide);
            }

            public Set<StoreBuilder<?>> stores() {
                return Collections.singleton(new StoreBuilder<>() {
                    @Override
                    public StoreBuilder<StateStore> withCachingEnabled() {
                        return null;
                    }

                    @Override
                    public StoreBuilder<StateStore> withCachingDisabled() {
                        return null;
                    }

                    @Override
                    public StoreBuilder<StateStore> withLoggingEnabled(Map<String, String> config) {
                        return null;
                    }

                    @Override
                    public StoreBuilder<StateStore> withLoggingDisabled() {
                        return null;
                    }

                    @Override
                    public StateStore build() {
                        return new InMemoryWindowDegreeStoreLinkedHashMap<>(ANNOTATE_NAME, scopeSize, scopeSlide, valueAndTimestampConstraintFactory);
                    }

                    @Override
                    public Map<String, String> logConfig() {
                        return null;
                    }

                    @Override
                    public boolean loggingEnabled() {
                        return false;
                    }

                    @Override
                    public String name() {
                        return ANNOTATE_NAME;
                    }
                });
            }
        }));
    }

    <KR, W extends Window> AnnTimeWindowedKStream<KR, V> groupAndWindowByNotWindowed(KeyValueMapper<K, V, KR> keySelector, Windows<W> slidingWindows, Grouped<KR, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>> grouped);

    <W extends Window> AnnTimeWindowedKStream<K, V> groupByKeyAndWindowBy(Windows<W> slidingWindows, Grouped<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>> grouped);

    <W extends Window> AnnTimeWindowedKStream<K, V> groupByKeyAndWindowBy(Windows<W> slidingWindows);
}
