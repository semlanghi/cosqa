package topkstreaming;

import org.apache.kafka.streams.KeyValue;

import java.util.Comparator;

/**
 * The helper instance to create the topK ranking based on streaming in events or table updates.
 *
 * @param <V> input record value
 */
public interface Ranker<V> {

    enum Order {
        ASCENDING,
        DESCENDING
    }

    /**
     * The order of ranking, either ascending or descending.
     */
    Order order();

    /**
     * The number of retained elements in given order.
     *
     * @return the total number of top records to be preserved.
     */
    int limit();


    Comparator<V> comparator();
}
