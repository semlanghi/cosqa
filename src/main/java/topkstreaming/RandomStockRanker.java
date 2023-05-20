package topkstreaming;

import stocks.Stock;

import java.util.Comparator;
import java.util.Random;

public class RandomStockRanker implements Ranker<Stock>{


    private int kParameter;
    private Order order;
    private Random random;

    public RandomStockRanker(int kParameter, Order order) {
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
    public Comparator<Stock> comparator() {
        return new Comparator<Stock>() {
            @Override
            public int compare(Stock o1, Stock o2) {
                return random.nextInt(-1, 2);
            }
        };
    }
}
