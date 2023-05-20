package annotation.degreestore;

import annotation.ConsistencyAnnotatedRecord;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.WindowStore;

import java.util.Iterator;

public interface InMemoryWindowDegreeStore<K,V> extends WindowStore<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>> {

    public Iterator<KeyValue<Windowed<K>, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>>> putWithReturn(K key, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>> value, long windowEndTimestamp);

    public KeyValue<Windowed<K>, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>> putWithReturnMostAnnotatedRecord(K key, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>> value, long windowEndTimestamp);

    public KeyValue<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>> putWithReturnMostAnnotatedRecordNotWindowed(K key, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>> value, long windowEndTimestamp);
}
