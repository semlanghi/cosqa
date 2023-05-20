package annotation;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import stocks.Stock;

import java.time.Duration;
import java.util.UUID;

public class AnnKStreamImpl<K,V> implements AnnKStream<K,V> {


    private KStream<K,ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>> internalKStream;

    public AnnKStreamImpl(KStream<K,ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>> kConsistencyAnnotatedRecordKStream) {
        this.internalKStream = kConsistencyAnnotatedRecordKStream;
    }

    @Override
    public KStream<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>> getInternalKStream() {
        return internalKStream;
    }

    @Override
    public KStream<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>> setInternalKStream(KStream<Windowed<Long>, ConsistencyAnnotatedRecord<ValueAndTimestamp<Pair<Stock, Stock>>>> transform) {
        return internalKStream;
    }




    @Override
    public AnnKStream<K, V> project(ValueMapper<V, V> valueMapper) {
        return new AnnKStreamImpl<>(internalKStream.mapValues(value ->
                value.withRecord(ValueAndTimestamp.make(valueMapper.apply(value.getWrappedRecord().value()), value.getWrappedRecord().timestamp()))));
    }

    @Override
    public AnnKStream<K, V> filter(Predicate<K, V> internalPredicate) {
        return new AnnKStreamImpl<>(internalKStream.filter((key, value) -> internalPredicate.test(key, value.getWrappedRecord().value())));
    }

    @Override
    public AnnKStream<K, V> filterNullValues() {
        return new AnnKStreamImpl<>(internalKStream.filter((key, value) -> value.getWrappedRecord() != null));
    }

    @Override
    public <VR> AnnKStream<K, VR> mapValues(ValueMapper<V, VR> valueMapper){
        return new AnnKStreamImpl<>(internalKStream.mapValues(value -> value.withRecord(ValueAndTimestamp.make(valueMapper.apply(value.getWrappedRecord().value()), value.getWrappedRecord().timestamp()))));
    }

    @Override
    public AnnKStream<K, V> filterOnAnnotation(Predicate<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>> predicate) {
        return new AnnKStreamImpl<>(internalKStream.filter(predicate));
    }

    @Override
    public <VR> AnnKStream<K, VR> join(AnnKStream<K, V> otherStream, ValueJoiner<V, V, VR> internalValueJoiner, JoinWindows joinWindows, Serde<K> keySerde,
                                      Serde<V> internalValueSerde) {
        return new AnnKStreamImpl<>(internalKStream.join(otherStream.getInternalKStream(), (value1, value2) -> {
            ValueAndTimestamp<VR> valueAndTimestamp = ValueAndTimestamp.make(internalValueJoiner.apply(value1.getWrappedRecord().value(), value2.getWrappedRecord().value())
                    , Math.max(value1.getWrappedRecord().timestamp(), value2.getWrappedRecord().timestamp()));
            return new ConsistencyAnnotatedRecord<>(value1.getPolynomial().times(value2.getPolynomial()), valueAndTimestamp);
        }, joinWindows, StreamJoined.with(keySerde, ConsistencyAnnotatedRecord.serde(internalValueSerde), ConsistencyAnnotatedRecord.serde(internalValueSerde))
                .withThisStoreSupplier(Stores.inMemoryWindowStore("join1"+ UUID.randomUUID(), Duration.ofMillis(joinWindows.size() + joinWindows.gracePeriodMs()), Duration.ofMillis(joinWindows.size()), true))
                .withOtherStoreSupplier(Stores.inMemoryWindowStore("join2"+UUID.randomUUID(), Duration.ofMillis(joinWindows.size() + joinWindows.gracePeriodMs()), Duration.ofMillis(joinWindows.size()), true))));
    }

    @Override
    public <VR> AnnKStream<K, VR> leftJoin(AnnKStream<K, V> otherStream, ValueJoiner<V, V, VR> internalValueJoiner, JoinWindows joinWindows, Serde<K> keySerde,
                                       Serde<V> internalValueSerde) {
        return new AnnKStreamImpl<>(internalKStream.leftJoin(otherStream.getInternalKStream(), (value1, value2) -> {
            ValueAndTimestamp<VR> valueAndTimestamp = ValueAndTimestamp.make(internalValueJoiner.apply(value1.getWrappedRecord().value(), value2.getWrappedRecord().value())
                    , Math.max(value1.getWrappedRecord().timestamp(), value2.getWrappedRecord().timestamp()));
            return new ConsistencyAnnotatedRecord<>(value1.getPolynomial().times(value2.getPolynomial()), valueAndTimestamp);
        }, joinWindows, StreamJoined.with(keySerde, ConsistencyAnnotatedRecord.serde(internalValueSerde), ConsistencyAnnotatedRecord.serde(internalValueSerde))
                .withThisStoreSupplier(Stores.inMemoryWindowStore("join1"+ UUID.randomUUID(), Duration.ofMillis(joinWindows.size() + joinWindows.gracePeriodMs()), Duration.ofMillis(joinWindows.size()), true))
                .withOtherStoreSupplier(Stores.inMemoryWindowStore("join2"+UUID.randomUUID(), Duration.ofMillis(joinWindows.size() + joinWindows.gracePeriodMs()), Duration.ofMillis(joinWindows.size()), true))));
    }

    @Override
    public <KR> AnnKStream<KR, V> selectKey(KeyValueMapper<K, ValueAndTimestamp<V>, KR> mapper){
        return new AnnKStreamImpl<>(internalKStream.selectKey(new KeyValueMapper<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>, KR>() {
            @Override
            public KR apply(K key, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>> value) {
                return mapper.apply(key, value.getWrappedRecord());
            }
        }));
    }

    @Override
    public <KR, W extends Window> AnnTimeWindowedKStream<KR, V> groupAndWindowBy(KeyValueMapper<K, V, KR> keySelector, Windows<W> slidingWindows, Grouped<KR, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>> grouped) {
        return new AnnTimeWindowedKStreamImpl<>(internalKStream
                .<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>>transform(new TransformerSupplier<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>, KeyValue<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>>>() {

                    @Override
                    public Transformer<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>, KeyValue<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>>> get() {
                        return new Transformer<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>, KeyValue<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>>>() {
                            ProcessorContext context;
                            @Override
                            public void init(ProcessorContext context) {
                                this.context = context;
                            }

                            @Override
                            public KeyValue<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>> transform(K key, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>> value) {
                                context.forward(key, value, To.all().withTimestamp(((Windowed<Long>)key).window().end() -1));
                                return null;
                            }

                            @Override
                            public void close() {

                            }
                        };
                    }
                })
                .groupBy((key, value) -> keySelector.apply(key, value.getWrappedRecord().value()), grouped).windowedBy(slidingWindows));
    }

    @Override
    public <KR, W extends Window> AnnTimeWindowedKStream<KR, V> groupAndWindowByNotWindowed(KeyValueMapper<K, V, KR> keySelector, Windows<W> slidingWindows, Grouped<KR, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>> grouped) {
        return new AnnTimeWindowedKStreamImpl<>(internalKStream
                .groupBy((key, value) -> keySelector.apply(key, value.getWrappedRecord().value()), grouped).windowedBy(slidingWindows));
    }

    @Override
    public <W extends Window> AnnTimeWindowedKStream<K, V> groupByKeyAndWindowBy(Windows<W> slidingWindows, Grouped<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>> grouped){
        return new AnnTimeWindowedKStreamImpl<>(internalKStream.groupByKey(grouped).windowedBy(slidingWindows));
    }

    @Override
    public <W extends Window> AnnTimeWindowedKStream<K, V> groupByKeyAndWindowBy(Windows<W> slidingWindows) {
        return new AnnTimeWindowedKStreamImpl<>(internalKStream.groupByKey().windowedBy(slidingWindows));
    }



}
