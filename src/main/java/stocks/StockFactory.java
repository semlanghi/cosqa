package stocks;

public class StockFactory {

    private Character stockName;

    public StockFactory(Character stockName) {
        this.stockName = stockName;
    }

    public Stock make(Double value, Long ts){
        return new Stock(stockName, value, ts);
    }

    public String getStockName() {
        return stockName.toString();
    }
}
