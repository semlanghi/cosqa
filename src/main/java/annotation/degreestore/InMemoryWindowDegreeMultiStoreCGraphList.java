package annotation.degreestore;

import annotation.ConsistencyAnnotatedRecord;
import annotation.cgraph.ConsistencyGraph;
import annotation.cgraph.ConsistencyGraphList;
import annotation.cgraph.ConsistencyNode;
import annotation.cgraph.InconsistencyGraphList;
import annotation.constraint.ConstraintFactory;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;

public class InMemoryWindowDegreeMultiStoreCGraphList<K,V> implements InMemoryWindowDegreeStore<K,V> {

    private final Logger logger = LoggerFactory.getLogger(InMemoryWindowDegreeMultiStoreCGraphList.class);
    private LinkedHashMap<Windowed<K>, ConsistencyGraph<V>> windowsSpeed;
    private LinkedHashMap<Windowed<K>, ConsistencyGraph<V>> windowsPK;
    private final long windowSize;
    private ProcessorContext context;
    private final String name;
    private boolean open;
    private ConstraintFactory<ValueAndTimestamp<V>> speedConstraintFactory;
    private ConstraintFactory<ValueAndTimestamp<V>> pkConstraintFactory;
    private final long windowSlide;
    private final long allowedLateness = 0;
    private final long cleanUpCounterMax = 1000;
    private long cleanUpCounter = 0;


    public InMemoryWindowDegreeMultiStoreCGraphList(String name, long windowSize, long windowSlide, ConstraintFactory<ValueAndTimestamp<V>> speedConstraintFactory, ConstraintFactory<ValueAndTimestamp<V>> pkConstraintFactory) {
        this.name = name;
        this.windowSize = windowSize;
        this.speedConstraintFactory = speedConstraintFactory;
        this.pkConstraintFactory = pkConstraintFactory;
        this.windowSlide = windowSlide;
    }

    public InMemoryWindowDegreeMultiStoreCGraphList(String name, long windowSize, long windowSlide, ConstraintFactory<ValueAndTimestamp<V>> speedConstraintFactory, String additionalSpeed) {
        this.name = name;
        this.windowSize = windowSize;
        this.speedConstraintFactory = speedConstraintFactory;
        this.pkConstraintFactory = null;
        this.windowSlide = windowSlide;
    }

    public InMemoryWindowDegreeMultiStoreCGraphList(String name, long windowSize, long windowSlide, ConstraintFactory<ValueAndTimestamp<V>> pkConstraintFactory) {
        this.name = name;
        this.windowSize = windowSize;
        this.speedConstraintFactory = null;
        this.pkConstraintFactory = pkConstraintFactory;
        this.windowSlide = windowSlide;
    }

    private void expireWindows(K key){
        if (context.timestamp() - windowSize - allowedLateness > 0){
            windowsSpeed.entrySet().removeIf(windowedLinkedHashMapEntry -> windowedLinkedHashMapEntry.getKey().window().endTime().isBefore(Instant.ofEpochMilli(context.timestamp() - windowSize - allowedLateness)));
            windowsPK.entrySet().removeIf(windowedLinkedHashMapEntry -> windowedLinkedHashMapEntry.getKey().window().endTime().isBefore(Instant.ofEpochMilli(context.timestamp() - windowSize - allowedLateness)));
        }
    }

    @Override
    public void put(K key, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>> value, long windowEndTimestamp) {
        if(++cleanUpCounter >= cleanUpCounterMax){
            expireWindows(key);
            cleanUpCounter = 0;
        }

        long windowStartTimestamp = Math.max(windowEndTimestamp - windowSize,0);
        windowEndTimestamp = Math.max(windowEndTimestamp, windowSize);
        Windowed<K> windowed;

        do {
            windowed = new Windowed<>(key, new TimeWindow(windowStartTimestamp, windowEndTimestamp));
            windowsSpeed.computeIfAbsent(windowed, k -> new ConsistencyGraphList<>(this.speedConstraintFactory));
            windowsSpeed.get(windowed).add(value);
            windowStartTimestamp+=windowSlide;
            windowEndTimestamp+=windowSlide;
        } while (windowStartTimestamp <= value.getWrappedRecord().timestamp());
    }

    /**
     * return all the elements belonging to a given window, identified by its starting and ending time
     * @param key
     * @param startTime
     * @param timeTo
     * @return
     */
    @Override
    public WindowStoreIterator<ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>> fetch(K key, long startTime, long timeTo) {
        if (startTime+windowSize!=timeTo)
            throw new RuntimeException("Window Boundaries not correct.");

        Windowed<K> windowed = new Windowed<>(key, new TimeWindow(Math.max(startTime,0), Math.max(timeTo, windowSize)));
        return new WindowStoreIteratorAdapter(windowsSpeed.get(windowed).getDebugNodeCollection().stream().map(ConsistencyNode::getConsistencyAnnotatedRecord).iterator());
    }

    @Override
    public KeyValueIterator<Windowed<K>, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>> fetch(K keyFrom, K keyTo, long timeFrom, long timeTo) {
        throw new UnsupportedOperationException("fetch() not supported.");
    }

    @Override
    public KeyValueIterator<Windowed<K>, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>> fetchAll(long timeFrom, long timeTo) {
        throw new UnsupportedOperationException("fetchAll() not supported.");
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void init(ProcessorContext context, StateStore root) {
        this.context = context;
        open = true;
        windowsSpeed = new LinkedHashMap<>();
        this.windowsPK = new LinkedHashMap<>();
        cleanUpCounter = 0;

        if (root != null) {
            context.register(root, (key, value) -> logger.info("Restoring the state store..."));
        }
    }

    @Override
    public void flush() {
        // do-nothing since it is in-memory
    }

    @Override
    public void close() {
        open = false;
        windowsSpeed.clear();
        windowsPK.clear();
    }

    @Override
    public boolean persistent() {
        return false;
    }

    @Override
    public boolean isOpen() {
        return open;
    }


    /**
     * Return only the last computed elements of a window identified by its starting time
     * @param key the key of the record of reference
     * @param endTime the starting time of the window of reference
     * @return
     */
    @Override
    public ConsistencyAnnotatedRecord<ValueAndTimestamp<V>> fetch(K key, long endTime) {
        Windowed<K> windowed = new Windowed<>(key, new TimeWindow(Math.max(endTime - windowSize,0), Math.max(endTime, windowSize)));
        return windowsSpeed.get(windowed).getDebugNodeCollection().stream().map(ConsistencyNode::getConsistencyAnnotatedRecord).iterator().next();
    }

    @Override
    public KeyValueIterator<Windowed<K>, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>> all() {
        throw new UnsupportedOperationException("all() not supported.");
    }

    @Override
    public Iterator<KeyValue<Windowed<K>, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>>> putWithReturn(K key, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>> originalValue, long windowEndTimestamp) {
        if(++cleanUpCounter >= cleanUpCounterMax){
            expireWindows(key);
            cleanUpCounter = 0;
        }

        long windowStartTimestamp = Math.max(windowEndTimestamp - windowSize,0);
        windowEndTimestamp = Math.max(windowEndTimestamp, windowSize);
        Windowed<K> windowed;

        List<KeyValue<Windowed<K>,ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>>> annotatedValues = new ArrayList<>();

        while (windowEndTimestamp <= originalValue.getWrappedRecord().timestamp()){
            windowStartTimestamp+=windowSlide;
            windowEndTimestamp+=windowSlide;
        }

        while (windowStartTimestamp <= originalValue.getWrappedRecord().timestamp() && windowEndTimestamp > originalValue.getWrappedRecord().timestamp()) {
            windowed = new Windowed<>(key, new TimeWindow(windowStartTimestamp, windowEndTimestamp));
            windowsSpeed.computeIfAbsent(windowed, k -> new ConsistencyGraphList<>(this.speedConstraintFactory));
            ConsistencyAnnotatedRecord<ValueAndTimestamp<V>> value = ConsistencyAnnotatedRecord.makeCopyOf(originalValue);
            windowsSpeed.get(windowed).add(value);
            annotatedValues.add(new KeyValue<>(windowed, value));
            windowStartTimestamp+=windowSlide;
            windowEndTimestamp+=windowSlide;
        }
        return annotatedValues.iterator();
    }

    @Override
    public KeyValue<Windowed<K>, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>> putWithReturnMostAnnotatedRecord(K key, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>> originalValue, long windowEndTimestamp) {
        if(++cleanUpCounter >= cleanUpCounterMax){
            expireWindows(key);
            cleanUpCounter = 0;
        }

        long windowStartTimestamp = Math.max(windowEndTimestamp - windowSize,0);
        windowEndTimestamp = Math.max(windowEndTimestamp, windowSize);
        Windowed<K> windowed;

        KeyValue<Windowed<K>,ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>> annotatedValue = null;
        boolean firstValueTaken = false;

        while (windowEndTimestamp <= originalValue.getWrappedRecord().timestamp()){
            windowStartTimestamp+=windowSlide;
            windowEndTimestamp+=windowSlide;
        }

        while (windowStartTimestamp <= originalValue.getWrappedRecord().timestamp() && windowEndTimestamp > originalValue.getWrappedRecord().timestamp()) {
            windowed = new Windowed<>(key, new TimeWindow(windowStartTimestamp, windowEndTimestamp));
            windowsSpeed.computeIfAbsent(windowed, k -> new ConsistencyGraphList<>(this.speedConstraintFactory));
            ConsistencyAnnotatedRecord<ValueAndTimestamp<V>> value = ConsistencyAnnotatedRecord.makeCopyOf(originalValue);
            windowsSpeed.get(windowed).add(value);
            if (!firstValueTaken){
                annotatedValue = new KeyValue<>(windowed, value);
                firstValueTaken = true;
            }
            windowStartTimestamp+=windowSlide;
            windowEndTimestamp+=windowSlide;
        }
        return annotatedValue;
    }

    @Override
    public KeyValue<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>> putWithReturnMostAnnotatedRecordNotWindowed(K key, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>> originalValue, long windowEndTimestamp) {

        KeyValue<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>> speedConsistencyAnnotatedRecordKeyValue = null;
        KeyValue<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>> pKConsistencyAnnotatedRecordKeyValue = null;
        if (speedConstraintFactory != null)
            speedConsistencyAnnotatedRecordKeyValue = getSpeedAnnotatedRecordKeyValue(key, originalValue, windowEndTimestamp);

        if (pkConstraintFactory != null)
            pKConsistencyAnnotatedRecordKeyValue = getPkAnnotatedRecordKeyValue(key, originalValue, windowEndTimestamp);


        if (speedConstraintFactory == null)
            return pKConsistencyAnnotatedRecordKeyValue;
        if (pkConstraintFactory == null)
            return speedConsistencyAnnotatedRecordKeyValue;


        ConsistencyAnnotatedRecord<ValueAndTimestamp<V>> consistencyAnnotatedRecord3 = new ConsistencyAnnotatedRecord<>(ValueAndTimestamp.make(originalValue.getWrappedRecord().value(), context.currentStreamTimeMs()));
        consistencyAnnotatedRecord3.setPolynomial(speedConsistencyAnnotatedRecordKeyValue.value.getPolynomial().times(pKConsistencyAnnotatedRecordKeyValue.value.getPolynomial()));

        return new KeyValue<>(key, consistencyAnnotatedRecord3);
    }

    private @Nullable KeyValue<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>> getSpeedAnnotatedRecordKeyValue(K key, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>> originalValue, long windowEndTimestamp) {
        if(++cleanUpCounter >= cleanUpCounterMax){
            expireWindows(key);
            cleanUpCounter = 0;
        }

        long windowStartTimestamp = Math.max(windowEndTimestamp - windowSize,0);
        windowEndTimestamp = Math.max(windowEndTimestamp, windowSize);
        Windowed<K> windowed;

//        List<KeyValue<Windowed<K>,ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>>> annotatedValues = new ArrayList<>();

        KeyValue<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>> kConsistencyAnnotatedRecordKeyValue = null;
        while (windowEndTimestamp <= originalValue.getWrappedRecord().timestamp()){
            windowStartTimestamp+=windowSlide;
            windowEndTimestamp +=windowSlide;
        }

        boolean assigned = false;
        while (windowStartTimestamp <= originalValue.getWrappedRecord().timestamp() && windowEndTimestamp > originalValue.getWrappedRecord().timestamp()) {
            windowed = new Windowed<>(key, new TimeWindow(windowStartTimestamp, windowEndTimestamp));
            windowsSpeed.computeIfAbsent(windowed, k -> new ConsistencyGraphList<>(this.speedConstraintFactory));
            ConsistencyAnnotatedRecord<ValueAndTimestamp<V>> value = ConsistencyAnnotatedRecord.makeCopyOf(originalValue);
            windowsSpeed.get(windowed).add(value);
            if (!assigned){
                kConsistencyAnnotatedRecordKeyValue = new KeyValue<>(windowed.key(), value);
                assigned = true;
            }
            windowStartTimestamp+=windowSlide;
            windowEndTimestamp +=windowSlide;
        }

        return kConsistencyAnnotatedRecordKeyValue;
    }

    private @Nullable KeyValue<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>> getPkAnnotatedRecordKeyValue(K key, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>> originalValue, long windowEndTimestamp) {
        if(++cleanUpCounter >= cleanUpCounterMax){
            expireWindows(key);
            cleanUpCounter = 0;
        }

        long windowStartTimestamp = Math.max(windowEndTimestamp - windowSize,0);
        windowEndTimestamp = Math.max(windowEndTimestamp, windowSize);
        Windowed<K> windowed;

//        List<KeyValue<Windowed<K>,ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>>> annotatedValues = new ArrayList<>();

        KeyValue<K, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>> kConsistencyAnnotatedRecordKeyValue = null;
        while (windowEndTimestamp <= originalValue.getWrappedRecord().timestamp()){
            windowStartTimestamp+=windowSlide;
            windowEndTimestamp +=windowSlide;
        }

        boolean assigned = false;
        while (windowStartTimestamp <= originalValue.getWrappedRecord().timestamp() && windowEndTimestamp > originalValue.getWrappedRecord().timestamp()) {
            windowed = new Windowed<>(key, new TimeWindow(windowStartTimestamp, windowEndTimestamp));
            windowsPK.computeIfAbsent(windowed, k -> new InconsistencyGraphList<>(this.pkConstraintFactory));
            ConsistencyAnnotatedRecord<ValueAndTimestamp<V>> value = ConsistencyAnnotatedRecord.makeCopyOf(originalValue);
            windowsPK.get(windowed).add(value);
            if (!assigned){
                kConsistencyAnnotatedRecordKeyValue = new KeyValue<>(windowed.key(), value);
                assigned = true;
            }
            windowStartTimestamp+=windowSlide;
            windowEndTimestamp +=windowSlide;
        }

        return kConsistencyAnnotatedRecordKeyValue;
    }


    protected class WindowStoreIteratorAdapter implements WindowStoreIterator<ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>> {

        private final Iterator<ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>> innerIt;

        private WindowStoreIteratorAdapter(Iterator<ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>> innerIt) {
            this.innerIt = innerIt;
        }

        @Override
        public boolean hasNext() {
            return innerIt.hasNext();
        }

        @Override
        public KeyValue<Long, ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>> next() {
            ConsistencyAnnotatedRecord<ValueAndTimestamp<V>> next = innerIt.next();
            return new KeyValue<>(next.getWrappedRecord().timestamp(), next);
        }

        @Override
        public void close() {
            //Do Nothing
        }

        @Override
        public Long peekNextKey() {
            throw new UnsupportedOperationException("peekNextKey() not supported in " + getClass().getName());
        }
    }
}
