package annotation;

import annotation.degreestore.InMemoryWindowDegreeStore;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.internals.WrappedStateStore;

public class ConsistencyAnnotatorDoubleConstraintSong<K, Vin> implements Transformer<K,Vin, KeyValue<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<Vin>>>> {
    private final long windowSize;
    private final long windowSlide;
    private ProcessorContext context;
    private WrappedStateStore<InMemoryWindowDegreeStore<K,Vin>, K, ConsistencyAnnotatedRecord<ValueAndTimestamp<Vin>>> degreeStore;
    private WrappedStateStore<InMemoryWindowDegreeStore<K,Vin>, K, ConsistencyAnnotatedRecord<ValueAndTimestamp<Vin>>> degreeStore2;
    private final String storeName;
    private final String storeName2;

    public ConsistencyAnnotatorDoubleConstraintSong(String storeName, String storeName2, long windowSize, long windowSlide) {
        this.storeName = storeName;
        this.storeName2 = storeName2;
        this.windowSize = windowSize;
        this.windowSlide = windowSlide;
    }

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        this.degreeStore = this.context.getStateStore(this.storeName);
        this.degreeStore2 = this.context.getStateStore(this.storeName2);
    }

    @Override
    public KeyValue<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<Vin>>> transform(K key, Vin value) {
        long windowEnd = (long) Math.ceil(((double)context.currentStreamTimeMs())/this.windowSlide)*windowSlide;

        // The annotation is performed directly on the record stored through its pointer, avoiding an unnecessary "fetch" operation
        ConsistencyAnnotatedRecord<ValueAndTimestamp<Vin>> consistencyAnnotatedRecord = new ConsistencyAnnotatedRecord<>(ValueAndTimestamp.make(value, context.currentStreamTimeMs()));
        KeyValue<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<Vin>>> value1 = degreeStore.wrapped().putWithReturnMostAnnotatedRecordNotWindowed(key, consistencyAnnotatedRecord, windowEnd);
        ConsistencyAnnotatedRecord<ValueAndTimestamp<Vin>> consistencyAnnotatedRecord2 = new ConsistencyAnnotatedRecord<>(ValueAndTimestamp.make(value, context.currentStreamTimeMs()));
        KeyValue<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<Vin>>> value2 = degreeStore2.wrapped().putWithReturnMostAnnotatedRecordNotWindowed(key, consistencyAnnotatedRecord2, windowEnd);

        ConsistencyAnnotatedRecord<ValueAndTimestamp<Vin>> consistencyAnnotatedRecord3 = new ConsistencyAnnotatedRecord<>(ValueAndTimestamp.make(value, context.currentStreamTimeMs()));
        consistencyAnnotatedRecord3.setPolynomial(value1.value.getPolynomial().times(value2.value.getPolynomial()));


        context.forward(value1.key , consistencyAnnotatedRecord3);

        return null;
    }


    @Override
    public void close() {
        degreeStore.close();
    }
}
