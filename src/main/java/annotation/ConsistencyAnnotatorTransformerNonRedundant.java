package annotation;

import annotation.degreestore.InMemoryWindowDegreeStore;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.internals.WrappedStateStore;

import java.util.Iterator;

public class ConsistencyAnnotatorTransformerNonRedundant<K, Vin> implements Transformer<K,Vin, KeyValue<Windowed<K>, ConsistencyAnnotatedRecord<ValueAndTimestamp<Vin>>>> {

    private final long windowSize;
    private final long windowSlide;
    private ProcessorContext context;
    private WrappedStateStore<InMemoryWindowDegreeStore<K,Vin>, K, ConsistencyAnnotatedRecord<ValueAndTimestamp<Vin>>> degreeStore;
    private final String storeName;

    public ConsistencyAnnotatorTransformerNonRedundant(String storeName, long windowSize, long windowSlide) {
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
    public KeyValue<Windowed<K>, ConsistencyAnnotatedRecord<ValueAndTimestamp<Vin>>> transform(K key, Vin value) {
        long windowEnd = (long) Math.ceil(((double)context.currentStreamTimeMs())/this.windowSlide)*windowSlide;

        // The annotation is performed directly on the record stored through its pointer, avoiding an unnecessary "fetch" operation
        ConsistencyAnnotatedRecord<ValueAndTimestamp<Vin>> consistencyAnnotatedRecord = new ConsistencyAnnotatedRecord<>(ValueAndTimestamp.make(value, context.currentStreamTimeMs()));
        Iterator<KeyValue<Windowed<K>, ConsistencyAnnotatedRecord<ValueAndTimestamp<Vin>>>> consistencyAnnotatedRecordIterator = degreeStore.wrapped().putWithReturn(key,
                consistencyAnnotatedRecord,
                windowEnd);

        while (consistencyAnnotatedRecordIterator.hasNext()){
            KeyValue<Windowed<K>, ConsistencyAnnotatedRecord<ValueAndTimestamp<Vin>>> next = consistencyAnnotatedRecordIterator.next();
//            if (next.value.getPolynomial().getMonomialsDegreeSum()>10000)
//                System.out.println("debug");
            context.forward(next.key , next.value);
        }


        return null;
    }


    @Override
    public void close() {
        degreeStore.close();
    }
}
