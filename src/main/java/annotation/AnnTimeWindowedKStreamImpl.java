package annotation;

import annotation.polynomial.Polynomial;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.WindowStore;

public class AnnTimeWindowedKStreamImpl<K,V> implements AnnTimeWindowedKStream<K,V> {

    private TimeWindowedKStream<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>> internalKGroupedStream;
    private long windowSize;
    private long retentionTime;

    public AnnTimeWindowedKStreamImpl(TimeWindowedKStream<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>> internalKGroupedStream) {
        this.internalKGroupedStream = internalKGroupedStream;
    }

    @Override
    public AnnKTable<Windowed<K>,V> reduce(final Reducer<V> reducer,
                                       final Materialized<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>, WindowStore<Bytes, byte[]>> materialized){
        return new AnnKTableImpl<>(internalKGroupedStream.reduce((value1, value2) -> {
            Polynomial resultPoly = value1.getPolynomial().plus(value2.getPolynomial());
            return value1
                    .withRecord(ValueAndTimestamp.make(reducer.apply(value1.getWrappedRecord().value(), value2.getWrappedRecord().value()), Math.max(value1.getWrappedRecord().timestamp(), value2.getWrappedRecord().timestamp())))
                    .withPolynomial(resultPoly);
        }, materialized));
    }

    @Override
    public <VR> AnnWindowedTable<K, VR> aggregate(final Initializer<VR> initializer, final Aggregator<K,V,VR> aggregator,
                                                  final Materialized<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<VR>>, WindowStore<Bytes, byte[]>> materialized){
        return new AnnWindowedTableImpl<K, VR>(internalKGroupedStream.aggregate(() -> new ConsistencyAnnotatedRecord<>(ValueAndTimestamp.make(initializer.apply(), 0L)), (key, value, aggregate) -> {
            Polynomial resultPoly = aggregate.getPolynomial().plus(value.getPolynomial());
            long ts = Math.max(value.getWrappedRecord().timestamp(), aggregate.getWrappedRecord().timestamp());
            VR aggregatedValue = aggregator.apply(key, value.getWrappedRecord().value(), aggregate.getWrappedRecord().value());
            return aggregate.withPolynomial(resultPoly).withRecord(ValueAndTimestamp.make(aggregatedValue, ts));
        }, materialized), windowSize, retentionTime);
    }
}
