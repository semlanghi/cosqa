package stocks;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.*;

public class PairStockSerde implements Serde<Pair<Stock, Stock>> {
    private Serde<Stock> stockSerde = StockSerde.instance();

    public PairStockSerde() {
    }

    public static Serde<Pair<Stock, Stock>> instance(){
        return new PairStockSerde();
    }

    @Override
    public Serializer<Pair<Stock, Stock>> serializer() {
        return new Serializer<Pair<Stock, Stock>>() {
            @Override
            public byte[] serialize(String topic, Pair<Stock, Stock> data) {
                try {
                    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    //                            ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
                    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    //                            BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(byteArrayOutputStream));

                    Stock left = data.getLeft();
                    Stock right = data.getRight();


                    dataOutputStream.writeLong(left.ts);
                    dataOutputStream.writeDouble(left.value);
                    dataOutputStream.writeChar(left.name);

                    dataOutputStream.writeLong(right.ts);
                    dataOutputStream.writeDouble(right.value);
                    dataOutputStream.writeChar(right.name);

                    byte[] bytes = byteArrayOutputStream.toByteArray();
//                            bufferedWriter.close();
                    dataOutputStream.close();
                    byteArrayOutputStream.close();
                    return bytes;


                } catch (IOException e) {
                    e.printStackTrace();
                }
                return null;
            }
        };
    }

    @Override
    public Deserializer<Pair<Stock, Stock>> deserializer() {
        return new Deserializer<Pair<Stock, Stock>>() {
            @Override
            public Pair<Stock, Stock> deserialize(String topic, byte[] data) {
                try {
                    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(data);
//                            ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
                    DataInputStream dataInputStream = new DataInputStream(byteArrayInputStream);
//                            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(byteArrayInputStream));

                    long ts = dataInputStream.readLong();
                    double v = dataInputStream.readDouble();
                    char name = dataInputStream.readChar();
//                            bufferedReader.close();
                    Stock left = new Stock(name, v, ts);

                    ts = dataInputStream.readLong();
                    v = dataInputStream.readDouble();
                    name = dataInputStream.readChar();
                    Stock right = new Stock(name, v, ts);


                    dataInputStream.close();
                    byteArrayInputStream.close();

                    return new ImmutablePair<>(left, right);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return null;
            }
        };
    }
}
