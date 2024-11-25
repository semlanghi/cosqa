package annotation;

import annotation.degreestore.InMemoryWindowDegreeStore;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.internals.WrappedStateStore;

public class ConsistencyAnnotatorTransformerFilter<K, Vin> implements Transformer<K,Vin, KeyValue<K, Vin>> {

    private final long windowSize;
    private final long windowSlide;
    private ProcessorContext context;
    private WrappedStateStore<InMemoryWindowDegreeStore<K,Vin>, K, ConsistencyAnnotatedRecord<ValueAndTimestamp<Vin>>> degreeStore;
    private final String storeName;

    public ConsistencyAnnotatorTransformerFilter(String storeName, long windowSize, long windowSlide) {
        this.storeName = storeName;
        this.windowSize = windowSize;
        this.windowSlide = windowSlide;
    }

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        this.degreeStore = this.context.getStateStore(this.storeName);
    }

    @Override
    public KeyValue<K, Vin> transform(K key, Vin value) {
        long windowEnd = (long) Math.ceil(((double)context.currentStreamTimeMs())/this.windowSlide)*windowSlide;

        // The annotation is performed directly on the record stored through its pointer, avoiding an unnecessary "fetch" operation
        ConsistencyAnnotatedRecord<ValueAndTimestamp<Vin>> consistencyAnnotatedRecord = new ConsistencyAnnotatedRecord<>(ValueAndTimestamp.make(value, context.currentStreamTimeMs()));
        KeyValue<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<Vin>>> value1 = degreeStore.wrapped().putWithReturnMostAnnotatedRecordNotWindowed(key, consistencyAnnotatedRecord, windowEnd);
        if (value1.value.getPolynomial().getDegree() <= 0)
            context.forward(value1.key , value1.value);

        return null;
    }


    @Override
    public void close() {
        degreeStore.close();
    }
}
