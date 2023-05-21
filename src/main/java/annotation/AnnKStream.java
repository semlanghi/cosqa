package annotation;

import annotation.constraint.ConstraintFactory;
import annotation.degreestore.InMemoryWindowDegreeStoreCGraph;
import annotation.degreestore.InMemoryWindowDegreeStoreCGraphList;
import annotation.degreestore.InMemoryWindowDegreeStoreLinkedHashMap;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import stocks.Stock;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

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
                                                                                ConstraintFactory<ValueAndTimestamp<V>> valueAndTimestampConstraintFactory) {
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
        }));
    }

    public static <K,V> AnnKStream<K,V> annotateListNotWindowed(KStream<K, V> stream, long scopeSize, long scopeSlide,
                                                                     ConstraintFactory<ValueAndTimestamp<V>> valueAndTimestampConstraintFactory) {
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
