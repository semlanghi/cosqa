package annotation;

import annotation.constraint.ConstraintFactory;
import annotation.constraint.StreamingConstraint;
import annotation.degreestore.InMemoryWindowDegreeStoreCGraph;
import annotation.degreestore.InMemoryWindowDegreeStoreCGraphList;
import annotation.degreestore.InMemoryWindowDegreeStoreLinkedHashMap;
import annotation.polynomial.MonomialImplString;
import annotation.polynomial.Polynomial;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import stocks.Stock;
import utils.ApplicationSupplier;
import utils.ExperimentConfig;
import utils.PerformanceInputInconsistencyTransformer;
import utils.PerformanceInputInconsistencyTransformerDummy;

import java.util.*;

public interface AnnKStream<K, V> {

    static final String ANNOTATE_NAME = "KSTREAM-ANNOTATE-STORE-"+ UUID.randomUUID();
    KStream<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>> getInternalKStream();

    KStream<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>> setInternalKStream(KStream<Windowed<Long>, ConsistencyAnnotatedRecord<ValueAndTimestamp<Pair<Stock, Stock>>>> transform);

    public AnnKStream<K,V> project(ValueMapper<V,V> valueMapper);

    AnnKStream<K, V> filter(Predicate<K, V> internalPredicate);

    AnnKStream<K, V> filterNullValues();

    <VR> AnnKStream<K, VR> mapValues(ValueMapper<V, VR> valueMapper);

    AnnKStream<K, V> filterOnAnnotation(Predicate<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>> predicate);

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
