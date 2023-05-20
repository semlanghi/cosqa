package annotation.cgraph;

import annotation.ConsistencyAnnotatedRecord;
import annotation.constraint.StreamingConstraint;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.util.ArrayList;
import java.util.List;

public class ConsistencyNode<V> {

    private ConsistencyAnnotatedRecord<ValueAndTimestamp<V>> consistencyAnnotatedRecord;
    private StreamingConstraint<ValueAndTimestamp<V>> constraint;
    private List<ConsistencyNode<V>> connectedNodes;
    private List<ConsistencyNode<V>> inverseConnections;
    private List<ConsistencyNode<V>> inconsistentNodes;

    public ConsistencyNode(ConsistencyAnnotatedRecord<ValueAndTimestamp<V>> consistencyAnnotatedRecord, StreamingConstraint<ValueAndTimestamp<V>> constraint) {
        this.consistencyAnnotatedRecord = consistencyAnnotatedRecord;
        this.constraint = constraint;
        this.connectedNodes = new ArrayList<>();
        this.inconsistentNodes = new ArrayList<>();
        this.inverseConnections = new ArrayList<>();
    }

    public void connectTo(ConsistencyNode<V> destination){
        connectedNodes.add(destination);
//        destination.connectInverse(this);
    }

    private void connectInverse(ConsistencyNode<V> destination){
        inverseConnections.add(destination);
    }

    public void connectInconsistency(ConsistencyNode<V> destination){
        inconsistentNodes.add(destination);
    }

    public double checkConstraint(ValueAndTimestamp<V> valueAndTimestamp){
        return constraint.checkConstraint(valueAndTimestamp);
    }

    public StreamingConstraint<ValueAndTimestamp<V>> getConstraint() {
        return constraint;
    }

    public ConsistencyAnnotatedRecord<ValueAndTimestamp<V>> getConsistencyAnnotatedRecord() {
        return consistencyAnnotatedRecord;
    }



    public List<ConsistencyNode<V>> getConnectedNodes() {
        return connectedNodes;
    }

    public List<ConsistencyNode<V>> getInverseConnections() {
        return inverseConnections;
    }

    public List<ConsistencyNode<V>> getInconsistentNodes() {
        return inconsistentNodes;
    }

    public boolean isConnected(){
        return !connectedNodes.isEmpty();
    }

    @Override
    public String toString() {
        return "ConsistencyNode{" + consistencyAnnotatedRecord.getWrappedRecord().toString() +"}";
    }

    public String toStringPrime() {
        return "ConsistencyNode{" + consistencyAnnotatedRecord.getWrappedRecord().toString() +
                    " connectedNodes=" + connectedNodes + "}\n";
    }
}
