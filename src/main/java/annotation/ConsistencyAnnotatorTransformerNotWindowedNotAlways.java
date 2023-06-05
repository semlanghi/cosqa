package annotation;

import annotation.degreestore.InMemoryWindowDegreeStore;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.internals.WrappedStateStore;

import java.util.Random;

public class ConsistencyAnnotatorTransformerNotWindowedNotAlways<K, Vin> implements Transformer<K,Vin, KeyValue<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<Vin>>>> {

    private final long windowSize;
    private final long windowSlide;
    private ProcessorContext context;
    private WrappedStateStore<InMemoryWindowDegreeStore<K,Vin>, K, ConsistencyAnnotatedRecord<ValueAndTimestamp<Vin>>> degreeStore;
    private final String storeName;
    private int constraintPercentage;
    private Random random;

    public ConsistencyAnnotatorTransformerNotWindowedNotAlways(String storeName, long windowSize, long windowSlide, int constraintPercentage) {
        this.storeName = storeName;
        this.windowSize = windowSize;
        this.windowSlide = windowSlide;
        this.constraintPercentage = constraintPercentage;
        this.random = new Random();
    }

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        this.degreeStore = this.context.getStateStore(this.storeName);
    }

    @Override
    public KeyValue<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<Vin>>> transform(K key, Vin value) {
        long windowEnd = (long) Math.ceil(((double)context.currentStreamTimeMs())/this.windowSlide)*windowSlide;

        // The annotation is performed directly on the record stored through its pointer, avoiding an unnecessary "fetch" operation
        ConsistencyAnnotatedRecord<ValueAndTimestamp<Vin>> consistencyAnnotatedRecord = new ConsistencyAnnotatedRecord<>(ValueAndTimestamp.make(value, context.currentStreamTimeMs()));
        if(random.nextInt(0, 100)<constraintPercentage){
            KeyValue<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<Vin>>> value1 = degreeStore.wrapped().putWithReturnMostAnnotatedRecordNotWindowed(key, consistencyAnnotatedRecord, windowEnd);
            context.forward(value1.key , value1.value);
        }else context.forward(key , consistencyAnnotatedRecord);


        return null;
    }


    @Override
    public void close() {
        degreeStore.close();
    }
}
