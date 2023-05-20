package annotation.cgraph;

import annotation.ConsistencyAnnotatedRecord;
import annotation.constraint.ConstraintFactory;
import annotation.constraint.SpeedConstraintDoubleValueFactory;
import annotation.constraint.StreamingConstraint;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.util.*;

public class ConsistencyGraphImpl<V> implements ConsistencyGraph<V> {

    private List<ConsistencyNode<V>> rootNodes;
    private ConstraintFactory<ValueAndTimestamp<V>> factory;


    public ConsistencyGraphImpl(ConstraintFactory<ValueAndTimestamp<V>> constraintFactory) {
        this.rootNodes = new LinkedList<>();
        this.factory = constraintFactory;
    }

    @Override
    public ConsistencyAnnotatedRecord<ValueAndTimestamp<V>> add(ValueAndTimestamp<V> dataPoint){

        ConsistencyAnnotatedRecord<ValueAndTimestamp<V>> consistencyAnnotatedRecord = new ConsistencyAnnotatedRecord<>(dataPoint);
        StreamingConstraint<ValueAndTimestamp<V>> make = factory.make(dataPoint);
        ConsistencyNode<V> consistencyNode = new ConsistencyNode<>(consistencyAnnotatedRecord, make);

        Iterator<ConsistencyNode<V>> iterator = rootNodes.iterator();

        Queue<ConsistencyNode<V>> nodeQueue = new ArrayDeque<>(rootNodes);

        while (nodeQueue.peek()!=null){
            ConsistencyNode<V> poll = nodeQueue.poll();

            //boolean flag to prevent calls to iterator.remove() which are not backed by a iterator.next() call
            boolean canRemove = false;
            if (iterator.hasNext()){
                iterator.next();
                canRemove = true;
            }

            double result = poll.checkConstraint(dataPoint);

            if (result < 0 || result > 0) {
                consistencyAnnotatedRecord.setPolynomial(consistencyNode.getConsistencyAnnotatedRecord().getPolynomial().times(poll.getConsistencyAnnotatedRecord().getWrappedRecord(), (int) Math.ceil(result)));
                nodeQueue.addAll(poll.getConnectedNodes());
            } else {
                consistencyNode.connectTo(poll);
                //Remove the connected node, as it is no more a root
                if (canRemove)
                    iterator.remove();
            }
        }

        rootNodes.add(consistencyNode);

        return consistencyAnnotatedRecord;
    }


    @Override
    public ConsistencyAnnotatedRecord<ValueAndTimestamp<V>> add(ConsistencyAnnotatedRecord<ValueAndTimestamp<V>> consistencyAnnotatedRecord){

        StreamingConstraint<ValueAndTimestamp<V>> make = factory.make(consistencyAnnotatedRecord.getWrappedRecord());

        ConsistencyNode<V> consistencyNode = new ConsistencyNode<>(consistencyAnnotatedRecord, make);

        Iterator<ConsistencyNode<V>> iterator = rootNodes.iterator();

        Queue<ConsistencyNode<V>> nodeQueue = new ArrayDeque<>(rootNodes);

        while (nodeQueue.peek()!=null){
            ConsistencyNode<V> poll = nodeQueue.poll();

            //boolean flag to prevent calls to iterator.remove() which are not backed by a iterator.next() call
            boolean canRemove = false;
            if (iterator.hasNext()){
                iterator.next();
                canRemove = true;
            }

            double result = poll.checkConstraint(consistencyAnnotatedRecord.getWrappedRecord());

            if (result < 0 || result > 0) {
                consistencyAnnotatedRecord.setPolynomial(consistencyNode.getConsistencyAnnotatedRecord().getPolynomial().times(poll.getConsistencyAnnotatedRecord().getWrappedRecord(), (int) Math.ceil(result)));
                nodeQueue.addAll(poll.getConnectedNodes());
            } else {
                consistencyNode.connectTo(poll);
                //Remove the connected node, as it is no more a root
                if (canRemove)
                    iterator.remove();
            }
        }

        rootNodes.add(consistencyNode);

        return consistencyAnnotatedRecord;
    }


    @Override
    public List<ConsistencyNode<V>> getDebugNodeCollection() {
        return null;
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        for (ConsistencyNode<V> tmp: rootNodes) {
            stringBuilder.append(tmp.toStringPrime());
        }
        return stringBuilder.toString();
    }

    public static void main(String[] args){

        ConsistencyGraph<Double> consistencyGraph = new ConsistencyGraphImpl<>(new SpeedConstraintDoubleValueFactory(0.3, -0.3));

        long time = 0L;

        consistencyGraph.add(ValueAndTimestamp.make(0.98, time));
        time++;

        System.out.println(consistencyGraph);

        consistencyGraph.add(ValueAndTimestamp.make(1.01, time));
        time++;

        System.out.println(consistencyGraph);

        consistencyGraph.add(ValueAndTimestamp.make(1.3, time));
        time++;

        System.out.println(consistencyGraph);

        consistencyGraph.add(ValueAndTimestamp.make(1.6, time));
        time++;

        System.out.println(consistencyGraph);

        consistencyGraph.add(ValueAndTimestamp.make(2.2, time));
        time++;

        System.out.println(consistencyGraph);

        consistencyGraph.add(ValueAndTimestamp.make(2.4, time));
        time++;

        System.out.println(consistencyGraph);

        consistencyGraph.add(ValueAndTimestamp.make(1.4, time));
        time++;

        System.out.println(consistencyGraph);
    }
}
