package topkstreaming;

import org.apache.commons.lang3.tuple.Pair;
import stocks.Stock;

import java.util.Comparator;
import java.util.Random;

public class RandomStockPairRanker implements Ranker<Pair<Stock,Stock>>{


    private int kParameter;
    private Order order;
    private Random random;

    public RandomStockPairRanker(int kParameter, Order order) {
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
    public Comparator<Pair<Stock,Stock>> comparator() {
        return new Comparator<Pair<Stock,Stock>>() {
            @Override
            public int compare(Pair<Stock,Stock> o1, Pair<Stock,Stock> o2) {
                return random.nextInt(-1, 2);
            }
        };
    }
}
