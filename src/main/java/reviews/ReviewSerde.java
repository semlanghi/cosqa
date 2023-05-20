package reviews;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.*;

public class ReviewSerde implements Serde<Review> {

    public ReviewSerde() {
    }

    public static ReviewSerde instance(){
        return new ReviewSerde();
    }

    @Override
    public Serializer<Review> serializer() {
        return new ReviewSerializer();
    }

    @Override
    public Deserializer<Review> deserializer() {
        return new ReviewDeserializer();
    }

    public static class ReviewSerializer implements Serializer<Review> {

        @Override
        public byte[] serialize(String topic, Review data) {
            try {
                ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
//                            ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
                DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
//                            BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(byteArrayOutputStream));

                dataOutputStream.writeLong(data.ts);
                dataOutputStream.writeInt(data.value);
                dataOutputStream.writeLong(data.userId);
                dataOutputStream.writeChars(data.title);




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

    public static class ReviewDeserializer implements Deserializer<Review> {
        @Override
        public Review deserialize(String topic, byte[] data) {
            try {
                ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(data);
//                            ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
                DataInputStream dataInputStream = new DataInputStream(byteArrayInputStream);
//                            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(byteArrayInputStream));

                Long ts = dataInputStream.readLong();
                int v = dataInputStream.readInt();
                BufferedReader d = new BufferedReader(new InputStreamReader(byteArrayInputStream));
                long userId = dataInputStream.readLong();
                String title = d.readLine();
//                            bufferedReader.close();

                d.close();
                dataInputStream.close();
                byteArrayInputStream.close();

                return new Review(userId, title, ts, v);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }
    }
}