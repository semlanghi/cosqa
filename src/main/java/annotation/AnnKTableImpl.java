package annotation;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import topkstreaming.Ranker;

import java.util.UUID;

public class AnnKTableImpl<K,V> implements AnnKTable<K,V> {

    private final KTable<K,ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>> internalKTable;

    public AnnKTableImpl(KTable<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>> internalKTable) {
        this.internalKTable = internalKTable;
    }

    @Override
    public KTable<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>> getInternalKTable() {
        return internalKTable;
    }

    @Override
    public AnnKStream<K,V> toStream(){
        return new AnnKStreamImpl<>(internalKTable.toStream());
    }


}
