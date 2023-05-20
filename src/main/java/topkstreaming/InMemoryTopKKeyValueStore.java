package topkstreaming;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class InMemoryTopKKeyValueStore<K,V> implements KeyValueStore<Windowed<K>, V> {

    //Map of the form <start of the window, heap>
    private final Logger logger = LoggerFactory.getLogger(InMemoryTopKKeyValueStore.class);
    private LinkedHashMap<Long, PriorityQueue<KeyValue<Windowed<K>, V>>> heapMap;
    private final Comparator<V> comparator;
    private final long retentionTime;
    private final int topK;
    private final String name;
    private ProcessorContext processorContext;
    private final long cleanUpCounterMax = 1000L;
    private long cleanUpCounter = 0L;
    private boolean open;

    public InMemoryTopKKeyValueStore(Comparator<V> comparator, long retentionTime, String name, int topK) {
        this.comparator = comparator;
        this.retentionTime = retentionTime;
        this.name = name;
        this.topK = topK;
    }



    @Override
    public void put(Windowed<K> key, V value) {
        cleanUpCounter++;
        if (cleanUpCounter>=cleanUpCounterMax)
            cleanUp(processorContext.timestamp() - retentionTime);
        heapMap.putIfAbsent(key.window().start(), new PriorityQueue<>(topK, new Comparator<KeyValue<Windowed<K>, V>>() {
            @Override
            public int compare(KeyValue<Windowed<K>, V> o1, KeyValue<Windowed<K>, V> o2) {
                return comparator.compare(o1.value, o2.value);
            }
        }));
        heapMap.get(key.window().start()).add(new KeyValue<>(key,value));
        if (heapMap.get(key.window().start()).size()>topK)
            heapMap.get(key.window().start()).poll();
    }

    private void cleanUp(long l) {
        heapMap.entrySet().removeIf(longPriorityQueueEntry -> longPriorityQueueEntry.getKey() < l);
    }

    @Override
    public V putIfAbsent(Windowed<K> key, V value) {
        throw new UnsupportedOperationException("TopK Store does not support this operation.");
    }

    @Override
    public void putAll(List<KeyValue<Windowed<K>, V>> entries) {
        for (KeyValue<Windowed<K>, V> tmp : entries
             ) {
            put(tmp.key, tmp.value);
        }
    }

    @Override
    public V delete(Windowed<K> key) {
        return null;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void init(ProcessorContext context, StateStore root) {
        this.processorContext = context;
        open = true;
        heapMap = new LinkedHashMap<>();

        if (root != null) {
            context.register(root, (key, value) -> logger.info("Restoring the state store..."));
        }
    }

    @Override
    public void flush() {
        //do nothing since it is in memory
    }

    @Override
    public void close() {
        heapMap.clear();
    }

    @Override
    public boolean persistent() {
        return false;
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    @Override
    public V get(Windowed<K> key) {
        return Objects.requireNonNull(heapMap.get(key.window().start()).peek()).value;
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> range(Windowed<K> from, Windowed<K> to) {
        if (!from.equals(to))
            throw new UnsupportedOperationException("from and to must be equal, as we are using this in substitution of a normal get");

        ArrayList<KeyValue<Windowed<K>, V>> keyValues = new ArrayList<>(heapMap.get(from.window().start()));
        Collections.reverse(keyValues);
        //Working around type system
        Iterator<KeyValue<Windowed<K>, V>> iterator = keyValues.iterator();
        return new TopkKeyValueIterator<>(iterator);
    }

    private static class TopkKeyValueIterator<K,V> implements KeyValueIterator<Windowed<K>, V>{

        Iterator<KeyValue<Windowed<K>, V>> innerIt;

        public TopkKeyValueIterator(Iterator<KeyValue<Windowed<K>, V>> innerIt) {
            this.innerIt = innerIt;
        }

        @Override
        public void close() {
            //doing nothing
        }

        @Override
        public Windowed<K> peekNextKey() {
            throw new UnsupportedOperationException("Not using multiple iterators in topK Store.");
        }

        @Override
        public boolean hasNext() {
            return innerIt.hasNext();
        }

        @Override
        public KeyValue<Windowed<K>, V> next() {
            return innerIt.next();
        }
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> all() {
        throw new UnsupportedOperationException("Not yet implemented.");
    }

    @Override
    public long approximateNumEntries() {
        throw new UnsupportedOperationException("cannot trackt he number of entries.");
    }
}
