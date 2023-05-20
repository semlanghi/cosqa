package annotation;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import topkstreaming.Ranker;

public interface AnnKTable<K, V> {

    KTable<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>> getInternalKTable();

    AnnKStream<K, V> toStream();
}
