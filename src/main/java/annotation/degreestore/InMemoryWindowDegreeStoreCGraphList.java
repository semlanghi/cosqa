package annotation.degreestore;

import annotation.ConsistencyAnnotatedRecord;
import annotation.constraint.ConstraintFactory;
import annotation.cgraph.ConsistencyGraph;
import annotation.cgraph.ConsistencyGraphList;
import annotation.cgraph.ConsistencyNode;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;

public class InMemoryWindowDegreeStoreCGraphList<K,V> implements InMemoryWindowDegreeStore<K,V> {

    private final Logger logger = LoggerFactory.getLogger(InMemoryWindowDegreeStoreCGraphList.class);
    private LinkedHashMap<Windowed<K>, ConsistencyGraph<V>> windows;
    private final long windowSize;
    private ProcessorContext context;
    private final String name;
    private boolean open;
    private final ConstraintFactory<ValueAndTimestamp<V>> constraintFactory;
    private final long windowSlide;
    private final long allowedLateness = 0;
    private final long cleanUpCounterMax = 1000;
    private long cleanUpCounter = 0;


    public InMemoryWindowDegreeStoreCGraphList(String name, long windowSize, long windowSlide, ConstraintFactory<ValueAndTimestamp<V>> constraintFactory) {
        this.name = name;
        this.windowSize = windowSize;
        this.constraintFactory = constraintFactory;
        this.windowSlide = windowSlide;
    }

    private void expireWindows(K key){
        if (context.timestamp() - windowSize - allowedLateness > 0){
            windows.entrySet().removeIf(windowedLinkedHashMapEntry -> windowedLinkedHashMapEntry.getKey().window().endTime().isBefore(Instant.ofEpochMilli(context.timestamp() - windowSize - allowedLateness)));
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
            windows.computeIfAbsent(windowed, k -> new ConsistencyGraphList<>(this.constraintFactory));
            windows.get(windowed).add(value);
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
        return new WindowStoreIteratorAdapter(windows.get(windowed).getDebugNodeCollection().stream().map(ConsistencyNode::getConsistencyAnnotatedRecord).iterator());
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
        windows = new LinkedHashMap<>();
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
        windows.clear();
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
        return windows.get(windowed).getDebugNodeCollection().stream().map(ConsistencyNode::getConsistencyAnnotatedRecord).iterator().next();
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
            windows.computeIfAbsent(windowed, k -> new ConsistencyGraphList<>(this.constraintFactory));
            ConsistencyAnnotatedRecord<ValueAndTimestamp<V>> value = ConsistencyAnnotatedRecord.makeCopyOf(originalValue);
            windows.get(windowed).add(value);
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
            windows.computeIfAbsent(windowed, k -> new ConsistencyGraphList<>(this.constraintFactory));
            ConsistencyAnnotatedRecord<ValueAndTimestamp<V>> value = ConsistencyAnnotatedRecord.makeCopyOf(originalValue);
            windows.get(windowed).add(value);
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
            windowEndTimestamp+=windowSlide;
        }

        boolean assigned = false;
        while (windowStartTimestamp <= originalValue.getWrappedRecord().timestamp() && windowEndTimestamp > originalValue.getWrappedRecord().timestamp()) {
            windowed = new Windowed<>(key, new TimeWindow(windowStartTimestamp, windowEndTimestamp));
            windows.computeIfAbsent(windowed, k -> new ConsistencyGraphList<>(this.constraintFactory));
            ConsistencyAnnotatedRecord<ValueAndTimestamp<V>> value = ConsistencyAnnotatedRecord.makeCopyOf(originalValue);
            windows.get(windowed).add(value);
            if (!assigned){
                kConsistencyAnnotatedRecordKeyValue = new KeyValue<>(windowed.key(), value);
                assigned = true;
            }
            windowStartTimestamp+=windowSlide;
            windowEndTimestamp+=windowSlide;
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
