package annotation;

import annotation.constraint.ConstraintFactory;
import annotation.constraint.StreamingConstraint;
import annotation.degreestore.InMemoryWindowDegreeStore;
import annotation.polynomial.MonomialImplString;
import annotation.polynomial.Polynomial;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.internals.WrappedStateStore;

public class ConsistencyAnnotatorMultiConstraintOnlyValues<K, Vin> implements ValueTransformer<Vin, ConsistencyAnnotatedRecord<ValueAndTimestamp<Vin>>> {
    private final long windowSize;
    private final long windowSlide;
    private ConstraintFactory<ValueAndTimestamp<Vin>> factory;
    private ProcessorContext context;
    private WrappedStateStore<InMemoryWindowDegreeStore<K,Vin>, K, ConsistencyAnnotatedRecord<ValueAndTimestamp<Vin>>> degreeStore;
    private final String storeName;
    private final K key;

    public ConsistencyAnnotatorMultiConstraintOnlyValues(long windowSize, long windowSlide, ConstraintFactory<ValueAndTimestamp<Vin>> factory, String storeName, K key) {
        this.storeName = storeName;
        this.windowSize = windowSize;
        this.windowSlide = windowSlide;
        this.factory = factory;
        this.key = key;
    }

    public ConsistencyAnnotatorMultiConstraintOnlyValues(long windowSize, long windowSlide, ConstraintFactory<ValueAndTimestamp<Vin>> factory, K key) {
        this.key = key;
        this.storeName = "";
        this.windowSize = windowSize;
        this.windowSlide = windowSlide;
        this.factory = factory;
    }

    public ConsistencyAnnotatorMultiConstraintOnlyValues(long windowSize, long windowSlide, String storeName, K key) {
        this.storeName = storeName;
        this.windowSize = windowSize;
        this.windowSlide = windowSlide;
        this.key = key;
        this.factory = null;
    }

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        if (!this.storeName.equals(""))
            this.degreeStore = this.context.getStateStore(this.storeName);
        else this.degreeStore = null;
    }

    @Override
    public ConsistencyAnnotatedRecord<ValueAndTimestamp<Vin>> transform(Vin value) {
        long windowEnd = (long) Math.ceil(((double)context.currentStreamTimeMs())/this.windowSlide)*windowSlide;

        // The annotation is performed directly on the record stored through its pointer, avoiding an unnecessary "fetch" operation
        ConsistencyAnnotatedRecord<ValueAndTimestamp<Vin>> consistencyAnnotatedRecord = new ConsistencyAnnotatedRecord<>(ValueAndTimestamp.make(value, context.currentStreamTimeMs()));
        KeyValue<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<Vin>>> value1;
        if (degreeStore != null)
            value1 = degreeStore.wrapped().putWithReturnMostAnnotatedRecordNotWindowed(key, consistencyAnnotatedRecord, windowEnd);
        else value1 = new KeyValue<>(key, consistencyAnnotatedRecord);


        ConsistencyAnnotatedRecord<ValueAndTimestamp<Vin>> consistencyAnnotatedRecord3;
        if (this.factory != null) {
            StreamingConstraint<ValueAndTimestamp<Vin>> make = factory.make(value1.value.getWrappedRecord());
            if (make.checkConstraint(value1.value.getWrappedRecord()) < 0){
                Polynomial polynomial = value1.value.getPolynomial();
                consistencyAnnotatedRecord3 = new ConsistencyAnnotatedRecord<>(ValueAndTimestamp.make(value, context.currentStreamTimeMs()));
                consistencyAnnotatedRecord3.setPolynomial(polynomial.times(new Polynomial(new MonomialImplString(make.getDescription(), 1))));
            } else consistencyAnnotatedRecord3 = value1.value;
        } else consistencyAnnotatedRecord3 = value1.value;

//        context.forward(key, consistencyAnnotatedRecord3);

        return consistencyAnnotatedRecord3;
    }


    @Override
    public void close() {
        if (degreeStore!=null)
            degreeStore.close();
    }
}
