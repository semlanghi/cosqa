package topkstreaming;

import gps.GPS;

import java.util.Comparator;
import java.util.Random;

public class RandomGPSRanker implements Ranker<GPS>{


    private int kParameter;
    private Order order;
    private Random random;

    public RandomGPSRanker(int kParameter, Order order) {
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
    public Comparator<GPS> comparator() {
        return new Comparator<GPS>() {
            @Override
            public int compare(GPS o1, GPS o2) {
                return random.nextInt(-1, 2);
            }
        };
    }
}
