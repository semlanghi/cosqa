package topkstreaming;

import reviews.Review;

import java.util.Comparator;
import java.util.Random;

public class RandomReviewRanker implements Ranker<Review>{


    private int kParameter;
    private Ranker.Order order;
    private Random random;

    public RandomReviewRanker(int kParameter, Order order) {
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
    public Comparator<Review> comparator() {
        return new Comparator<Review>() {
            @Override
            public int compare(Review o1, Review o2) {
                return random.nextInt(-1, 2);
            }
        };
    }
}
