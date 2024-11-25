package stocks;

import org.apache.kafka.common.serialization.Serde;

import java.util.Objects;

public class Stock {
    Character name;
    Double value;
    Long ts;

    public Stock(Character name, Double value, Long ts) {
        this.name = name;
        this.value = value;
        this.ts = ts;
    }

    public Character getName() {
        return name;
    }

    public Double getValue() {
        return value;
    }

    public Long getTs() {
        return ts;
    }

    public void dirty(){
        this.value = 1000.0;
    }

    public static void main(String[] args){
        Stock stock = new Stock('A', 12.3, 12L);
        Stock stock1 = new Stock('G', 27.3, 13L);
        Stock stock2 = new Stock('G', 56.4, 27L);

        Serde<Stock> serde = StockSerde.instance();

        byte[] stockByte = serde.serializer().serialize("csbdjc", stock);
        byte[] stock1Byte = serde.serializer().serialize("csbdjc", stock1);
        byte[] stock2Byte = serde.serializer().serialize("csbdjc", stock2);

        System.out.println(serde.deserializer().deserialize("csbdjc", stockByte));
        System.out.println(serde.deserializer().deserialize("csbdjc", stock1Byte));
        System.out.println(serde.deserializer().deserialize("csbdjc", stock2Byte));

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Stock)) return false;
        Stock stock = (Stock) o;
        return Objects.equals(name, stock.name) && Objects.equals(ts, stock.ts);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, ts);
    }

    @Override
    public String toString() {
        return "Stock{" +
                "name=" + name +
                ", value=" + value +
                ", ts=" + ts +
                '}';
    }

    public void addTimestamp(long l) {
        this.ts += l;
    }

    public void setValue(double repaired) {
        this.value = repaired;
    }
}
