package annotation;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.WindowStore;

public interface AnnTimeWindowedKStream<K, V> {
    AnnKTable<Windowed<K>, V> reduce(Reducer<V> reducer,
                                     Materialized<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>, WindowStore<Bytes, byte[]>> materialized);

    <VR> AnnWindowedTable<K, VR> aggregate(Initializer<VR> initializer, Aggregator<K, V, VR> aggregator,
                                           Materialized<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<VR>>, WindowStore<Bytes, byte[]>> materialized);
}
