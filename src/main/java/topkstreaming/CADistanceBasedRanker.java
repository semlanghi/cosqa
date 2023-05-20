package topkstreaming;

import annotation.ConsistencyAnnotatedRecord;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.util.Comparator;

public class CADistanceBasedRanker implements Ranker<ConsistencyAnnotatedRecord<ValueAndTimestamp<? extends Object>>>{

    private int kParameter;
    private Order order;
    private int inconsistencyDegreeGranularity;

    public CADistanceBasedRanker(int kParameter, Order order) {
        this.kParameter = kParameter;
        this.order = order;
    }

    @Override
    public Order order() {
        return this.order;
    }

    @Override
    public int limit() {
        return kParameter;
    }


    @Override
    public Comparator<ConsistencyAnnotatedRecord<ValueAndTimestamp<?>>> comparator() {
        if (order == Order.DESCENDING)
            return new Comparator<ConsistencyAnnotatedRecord<ValueAndTimestamp<?>>>() {
                @Override
                public int compare(ConsistencyAnnotatedRecord<ValueAndTimestamp<?>> o1, ConsistencyAnnotatedRecord<ValueAndTimestamp<?>> o2) {
                    return Integer.compare(o1.getPolynomial().getMonomialsDegreeSum(), o2.getPolynomial().getMonomialsDegreeSum());
                }
            };
        else return new Comparator<ConsistencyAnnotatedRecord<ValueAndTimestamp<?>>>() {
                @Override
                public int compare(ConsistencyAnnotatedRecord<ValueAndTimestamp<?>> o1, ConsistencyAnnotatedRecord<ValueAndTimestamp<?>> o2) {
                    return -Integer.compare(o1.getPolynomial().getMonomialsDegreeSum(), o2.getPolynomial().getMonomialsDegreeSum());
                }
            };
    }


}
