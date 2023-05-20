package annotation;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import topkstreaming.*;
import utils.ApplicationSupplier;

import java.util.*;

public class AnnWindowedTableImpl<K,V> implements AnnWindowedTable<K,V>{

    public static final String TOP_K_NAME = "KTABLE-TOPK-STORE-"+ UUID.randomUUID();

    private KTable<Windowed<K>, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>> internalKTable;
    private long windowSize;
    private long retentionTime;

    public AnnWindowedTableImpl(KTable<Windowed<K>, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>> internalKTable, long windowSize, long retentionTime) {
        this.internalKTable = internalKTable;
        this.windowSize = windowSize;
        this.retentionTime = retentionTime;
    }

    @Override
    public KTable<Windowed<K>, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>> getInternalKTable() {
        return internalKTable;
    }

    @Override
    public KTable<Windowed<K>, Integer> top(CADistanceBasedRanker ranker, ApplicationSupplier applicationSupplier, Properties properties) {
        return this.internalKTable.transformValues(new ValueTransformerWithKeySupplier<Windowed<K>, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>, Integer>() {
            @Override
            public ValueTransformerWithKey<Windowed<K>, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>, Integer> get() {
                return new TopKCATransformer<>(TOP_K_NAME, applicationSupplier, properties);
            }
        }, TOP_K_NAME);
    }

    @Override
    public void topKProcessorVisualizer(CADistanceBasedRanker ranker, StreamsBuilder builder, TimeWindows timeWindows, ApplicationSupplier shutdownHook, Properties properties) {
        builder.addStateStore(new StoreBuilder<>() {
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
                return new InMemoryTopKKeyValueStore<>(ranker.comparator(), timeWindows.sizeMs*10, TOP_K_NAME, ranker.limit());
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
                return TOP_K_NAME;
            }
        });
        this.internalKTable.toStream().process(new ProcessorSupplier<Windowed<K>, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>, Void, Void>() {
            @Override
            public Processor<Windowed<K>, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>, Void, Void> get() {
                return new TopKCAProcessor<>(TOP_K_NAME, shutdownHook, properties);
            }
        }, TOP_K_NAME);
    }

}
