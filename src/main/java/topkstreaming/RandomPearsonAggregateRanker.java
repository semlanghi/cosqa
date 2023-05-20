package topkstreaming;


import stocks.PearsonAggregate;

import java.util.Comparator;
import java.util.Random;

public class RandomPearsonAggregateRanker implements Ranker<PearsonAggregate>{


    private int kParameter;
    private Order order;
    private Random random;

    public RandomPearsonAggregateRanker(int kParameter, Order order) {
        this.kParameter = kParameter;
        this.order = order;
        this.random = new Random();
    }

    @Override
    public Order order() {
        return order;
    }

    @Override
    public int limit() {
        return kParameter;
    }

    @Override
    public Comparator<PearsonAggregate> comparator() {
        return new Comparator<PearsonAggregate>() {
            @Override
            public int compare(PearsonAggregate o1, PearsonAggregate o2) {
                return random.nextInt(-1, 2);
            }
        };
    }
}
