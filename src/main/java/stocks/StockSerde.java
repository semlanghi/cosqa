package stocks;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.*;

public class StockSerde implements Serde<Stock> {

    public StockSerde() {
    }

    public static StockSerde instance(){
        return new StockSerde();
    }

    @Override
    public Serializer<Stock> serializer() {
        return new StockSerializer();
    }

    @Override
    public Deserializer<Stock> deserializer() {
        return new StockDeserializer();
    }

    public static class StockSerializer implements Serializer<Stock> {

        @Override
        public byte[] serialize(String topic, Stock data) {
            try {
                ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
//                            ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
                DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
//                            BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(byteArrayOutputStream));

                dataOutputStream.writeLong(data.ts);
                dataOutputStream.writeDouble(data.value);
                dataOutputStream.writeChar(data.name);




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
    }

    public static class StockDeserializer implements Deserializer<Stock> {
        @Override
        public Stock deserialize(String topic, byte[] data) {
            try {
                ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(data);
//                            ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
                DataInputStream dataInputStream = new DataInputStream(byteArrayInputStream);
//                            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(byteArrayInputStream));

                Long ts = dataInputStream.readLong();
                double v = dataInputStream.readDouble();
                Character name = dataInputStream.readChar();
//                            bufferedReader.close();
                dataInputStream.close();
                byteArrayInputStream.close();

                return new Stock(name, v, ts);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }
    }
}