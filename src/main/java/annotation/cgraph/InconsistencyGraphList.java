package annotation.cgraph;

import annotation.ConsistencyAnnotatedRecord;
import annotation.constraint.ConstraintFactory;
import annotation.constraint.PrimaryKeyConstraint;
import annotation.constraint.SpeedConstraintDoubleValueFactory;
import annotation.constraint.StreamingConstraint;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.math3.util.Pair;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class InconsistencyGraphList<V> extends ConsistencyGraphList<V>{
    public InconsistencyGraphList(ConstraintFactory<ValueAndTimestamp<V>> constraintFactory) {
        super(constraintFactory);
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
            Set<ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>> inconsistentRecordsWithoutRedundancy = new HashSet<>();

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

                        if (resultInt == 0) {
                            consistencyAnnotatedRecord.setPolynomial(consistencyAnnotatedRecord.getPolynomial().times(originPoint, resultInt));
                            indexOfTraversal[i]++;
                            canContinue = true;
                        } else {
                            attached = true;
                            if (indexOfTraversal[i] == 0)
                                consistencyAnnotatedRecords.add(consistencyAnnotatedRecord);
                            else {
                                //Create new path only if the new path has not been created yet
                                if (!inconsistentRecordsWithoutRedundancy.contains(valueAndTimestampConsistencyAnnotatedRecord)){
                                    List<ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>> nwPath = new ArrayList<>();
                                    nwPath.add(valueAndTimestampConsistencyAnnotatedRecord);
                                    nwPath.add(consistencyAnnotatedRecord);
                                    paths.add(nwPath);
                                    inconsistentRecordsWithoutRedundancy.add(valueAndTimestampConsistencyAnnotatedRecord);
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
            Set<ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>> inconsistentRecordsWithoutRedundancy = new HashSet<>();

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

                        if (resultInt == 0) {
                            consistencyAnnotatedRecord.setPolynomial(consistencyAnnotatedRecord.getPolynomial().times(make1, resultInt));
                            indexOfTraversal[i]++;
                            canContinue = true;
                        } else {
                            attached = true;
                            if (indexOfTraversal[i] == 0)
                                consistencyAnnotatedRecords.add(consistencyAnnotatedRecord);
                            else {
                                //Create new path only if the new path has not been created yet
                                if (!inconsistentRecordsWithoutRedundancy.contains(valueAndTimestampConsistencyAnnotatedRecord)){
                                    List<ConsistencyAnnotatedRecord<ValueAndTimestamp<V>>> nwPath = new ArrayList<>();
                                    nwPath.add(valueAndTimestampConsistencyAnnotatedRecord);
                                    nwPath.add(consistencyAnnotatedRecord);
                                    paths.add(nwPath);
                                    inconsistentRecordsWithoutRedundancy.add(valueAndTimestampConsistencyAnnotatedRecord);
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
    public String toString() {
        return "InconsistencyGraphList{paths:"+paths+"}";
    }

    public static void main(String[] args){
        ConsistencyGraph<Pair<String, Double>> consistencyGraph = new InconsistencyGraphList<>(new ConstraintFactory<ValueAndTimestamp<Pair<String, Double>>>() {
            @Override
            public StreamingConstraint<ValueAndTimestamp<Pair<String, Double>>> make(ValueAndTimestamp<Pair<String, Double>> origin) {
                return new PrimaryKeyConstraint<String, Pair<String, Double>>(origin) {
                    @Override
                    protected String getRecordKey(Pair<String, Double> value) {
                        return value.getFirst();
                    }
                };
            }
        });

        long time = 0L;

        consistencyGraph.add(ValueAndTimestamp.make(new Pair<>("k", 0.98), time));
        time++;

        System.out.println(consistencyGraph);

        consistencyGraph.add(ValueAndTimestamp.make(new Pair<>("g", 1.1), time));
        time++;

        System.out.println(consistencyGraph);

        consistencyGraph.add(ValueAndTimestamp.make(new Pair<>("k", 1.3), time));
        time++;

        System.out.println(consistencyGraph);

        consistencyGraph.add(ValueAndTimestamp.make(new Pair<>("k", 1.6), time));
        time++;

        System.out.println(consistencyGraph);

        consistencyGraph.add(ValueAndTimestamp.make(new Pair<>("k", 2.2), time));
        time++;

        System.out.println(consistencyGraph);

        consistencyGraph.add(ValueAndTimestamp.make(new Pair<>("k", 2.4), time));
        time++;

        System.out.println(consistencyGraph);

        consistencyGraph.add(ValueAndTimestamp.make(new Pair<>("k", 1.4), time));
        time++;

        System.out.println(consistencyGraph);
    }
}
