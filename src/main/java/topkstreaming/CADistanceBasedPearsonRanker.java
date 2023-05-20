package topkstreaming;

import annotation.ConsistencyAnnotatedRecord;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import stocks.PearsonAggregate;

import java.util.Comparator;

public class CADistanceBasedPearsonRanker implements Ranker<ConsistencyAnnotatedRecord<ValueAndTimestamp<PearsonAggregate>>>{

    private int kParameter;
    private Order order;
    private int inconsistencyDegreeGranularity;

    public CADistanceBasedPearsonRanker(int kParameter, Order order) {
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
    public Comparator<ConsistencyAnnotatedRecord<ValueAndTimestamp<PearsonAggregate>>> comparator() {
        if (order == Order.DESCENDING)
            return new Comparator<ConsistencyAnnotatedRecord<ValueAndTimestamp<PearsonAggregate>>>() {
                @Override
                public int compare(ConsistencyAnnotatedRecord<ValueAndTimestamp<PearsonAggregate>> o1, ConsistencyAnnotatedRecord<ValueAndTimestamp<PearsonAggregate>> o2) {
                    return Integer.compare(o1.getPolynomial().getMonomialsDegreeSum(), o2.getPolynomial().getMonomialsDegreeSum());
                }
            };
        else return new Comparator<ConsistencyAnnotatedRecord<ValueAndTimestamp<PearsonAggregate>>>() {
            @Override
            public int compare(ConsistencyAnnotatedRecord<ValueAndTimestamp<PearsonAggregate>> o1, ConsistencyAnnotatedRecord<ValueAndTimestamp<PearsonAggregate>> o2) {
                return -Integer.compare(o1.getPolynomial().getMonomialsDegreeSum(), o2.getPolynomial().getMonomialsDegreeSum());
            }
        };
    }


}
