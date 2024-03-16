package annotation.cgraph;

import annotation.ConsistencyAnnotatedRecord;
import annotation.constraint.ConstraintFactory;
import annotation.constraint.SpeedConstraintDoubleValueFactory;
import annotation.constraint.StreamingConstraint;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.util.*;

public class ConsistencyGraphList<V> implements ConsistencyGraph<V> {

    private ConstraintFactory<ValueAndTimestamp<V>> factory;
    private List<List<ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>>> paths;


    public ConsistencyGraphList(ConstraintFactory<ValueAndTimestamp<V>> constraintFactory) {
        this.paths = new ArrayList<>();
        this.factory = constraintFactory;
    }



    @Override
    public ConsistencyAnnotatedRecord<ValueAndTimestamp<V>> add(ValueAndTimestamp<V> dataPoint){

        ConsistencyAnnotatedRecord<ValueAndTimestamp<V>> consistencyAnnotatedRecord = new ConsistencyAnnotatedRecord<>(dataPoint);
        StreamingConstraint<ValueAndTimestamp<V>> make = factory.make(dataPoint);

        int size = paths.size();
        if(size >0) {
            boolean[] pathsClosed = new boolean[size];
            int[] indexOfTraversal = new int[size];
            boolean canContinue = true, attached = false;
            Set<ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>> consistentRecordsWithoutRedundancy = new HashSet<>();

            while (canContinue) {
                canContinue = false;
                for (int i = 0; i < size; i++) {
                    if (indexOfTraversal[i] >= paths.get(i).size())
                        pathsClosed[i] = true;
                    if (!pathsClosed[i]) {
                        List<ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>> consistencyAnnotatedRecords = paths.get(i);
                        int sizeOfPath = consistencyAnnotatedRecords.size();
                        ConsistencyAnnotatedRecord<ValueAndTimestamp<V>> valueAndTimestampConsistencyAnnotatedRecord = consistencyAnnotatedRecords.get(sizeOfPath - 1 - indexOfTraversal[i]);
                        ValueAndTimestamp<V> originPoint = valueAndTimestampConsistencyAnnotatedRecord.getWrappedRecord();
                        double result = factory.make(originPoint).checkConstraint(dataPoint);
                        int resultInt = (int) Math.ceil(Math.abs(result));

                        if (resultInt != 0) {
                            consistencyAnnotatedRecord.setPolynomial(consistencyAnnotatedRecord.getPolynomial().times(originPoint, resultInt));
                            indexOfTraversal[i]++;
                            canContinue = true;
                        } else {
                            attached = true;
                            if (indexOfTraversal[i] == 0)
                                consistencyAnnotatedRecords.add(consistencyAnnotatedRecord);
                            else {
                                //Create new path only if the new path has not been created yet
                                if (!consistentRecordsWithoutRedundancy.contains(valueAndTimestampConsistencyAnnotatedRecord)){
                                    List<ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>> nwPath = new ArrayList<>();
                                    nwPath.add(valueAndTimestampConsistencyAnnotatedRecord);
                                    nwPath.add(consistencyAnnotatedRecord);
                                    paths.add(nwPath);
                                    consistentRecordsWithoutRedundancy.add(valueAndTimestampConsistencyAnnotatedRecord);
                                }
                            }
                            pathsClosed[i] = true;
                        }
                    }
                }
            }
            if (!attached){
                List<ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>> nwPath = new ArrayList<>();
                nwPath.add(consistencyAnnotatedRecord);
                paths.add(nwPath);
            }
        } else {
            List<ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>> nwPath = new ArrayList<>();
            nwPath.add(consistencyAnnotatedRecord);
            paths.add(nwPath);
        }

        return consistencyAnnotatedRecord;
    }

    @Override
    public ConsistencyAnnotatedRecord<ValueAndTimestamp<V>> add(ConsistencyAnnotatedRecord<ValueAndTimestamp<V>> consistencyAnnotatedRecord){

        StreamingConstraint<ValueAndTimestamp<V>> make = factory.make(consistencyAnnotatedRecord.getWrappedRecord());

        int size = paths.size();
        if(size >0) {
            boolean[] pathsClosed = new boolean[size];
            int[] indexOfTraversal = new int[size];
            boolean canContinue = true, attached = false;
            Set<ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>> consistentRecordsWithoutRedundancy = new HashSet<>();

            while (canContinue) {
                canContinue = false;
                for (int i = 0; i < size; i++) {
                    if (indexOfTraversal[i] >= paths.get(i).size())
                        pathsClosed[i] = true;
                    if (!pathsClosed[i]) {
                        List<ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>> consistencyAnnotatedRecords = paths.get(i);
                        int sizeOfPath = consistencyAnnotatedRecords.size();
                        ConsistencyAnnotatedRecord<ValueAndTimestamp<V>> valueAndTimestampConsistencyAnnotatedRecord = consistencyAnnotatedRecords.get(sizeOfPath - 1 - indexOfTraversal[i]);
                        ValueAndTimestamp<V> originPoint = valueAndTimestampConsistencyAnnotatedRecord.getWrappedRecord();
                        StreamingConstraint<ValueAndTimestamp<V>> make1 = factory.make(originPoint);
                        double result = make1.checkConstraint(consistencyAnnotatedRecord.getWrappedRecord());
                        int resultInt = (int) Math.ceil(Math.abs(result));

                        if (resultInt != 0) {
                            consistencyAnnotatedRecord.setPolynomial(consistencyAnnotatedRecord.getPolynomial().times(make1, resultInt));
                            indexOfTraversal[i]++;
                            canContinue = true;
                        } else {
                            attached = true;
                            if (indexOfTraversal[i] == 0)
                                consistencyAnnotatedRecords.add(consistencyAnnotatedRecord);
                            else {
                                //Create new path only if the new path has not been created yet
                                if (!consistentRecordsWithoutRedundancy.contains(valueAndTimestampConsistencyAnnotatedRecord)){
                                    List<ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>> nwPath = new ArrayList<>();
                                    nwPath.add(valueAndTimestampConsistencyAnnotatedRecord);
                                    nwPath.add(consistencyAnnotatedRecord);
                                    paths.add(nwPath);
                                    consistentRecordsWithoutRedundancy.add(valueAndTimestampConsistencyAnnotatedRecord);
                                }
                            }
                            pathsClosed[i] = true;
                        }
                    }
                }
            }
            if (!attached){
                List<ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>> nwPath = new ArrayList<>();
                nwPath.add(consistencyAnnotatedRecord);
                paths.add(nwPath);
            }
        } else {
            List<ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>> nwPath = new ArrayList<>();
            nwPath.add(consistencyAnnotatedRecord);
            paths.add(nwPath);
        }

        return consistencyAnnotatedRecord;
    }

    @Override
    public List<ConsistencyNode<V>> getDebugNodeCollection() {
        return null;
    }

    @Override
    public String toString() {
        return "ConsistencyGraphList{" +
                "paths=" + paths +
                '}';
    }

    public static void main(String[] args){
        ConsistencyGraph<Double> consistencyGraph = new ConsistencyGraphList<>(new SpeedConstraintDoubleValueFactory(0.3, -0.3));

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
