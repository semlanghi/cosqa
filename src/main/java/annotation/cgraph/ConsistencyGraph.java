package annotation.cgraph;

import annotation.ConsistencyAnnotatedRecord;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.jetbrains.annotations.VisibleForTesting;

import java.util.List;

/**
 * The consistency graph is a representation that exploits {@link annotation.constraint.SpeedConstraint} backward
 * transitivity, maintaining efficiency independently of the number of records within the graph.
 * @param <V> the type of value of the record
 */
public interface ConsistencyGraph<V> {

    /**
     * The add operation consists on two steps: (i) annotating the record and (ii) adding the record to the graph.
     * The complexity of this operation is O(I), where I is the number of inconsistencies in the graph.
     * @param consistencyAnnotatedRecord the record to add in the graph
     * @return the annotated record
     */
    ConsistencyAnnotatedRecord<ValueAndTimestamp<V>> add(ConsistencyAnnotatedRecord<ValueAndTimestamp<V>> consistencyAnnotatedRecord);

    /**
     * A getter to return the whole set of graph nodes
     * @return a graph nodes list
     */
    List<ConsistencyNode<V>> getDebugNodeCollection();

    @VisibleForTesting
    public ConsistencyAnnotatedRecord<ValueAndTimestamp<V>> add(ValueAndTimestamp<V> dataPoint);
}
