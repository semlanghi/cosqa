package linearroad;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.*;

public class SpeedEventSerde implements Serde<SpeedEvent> {

    public SpeedEventSerde() {
    }

    public static SpeedEventSerde instance(){
        return new SpeedEventSerde();
    }

    @Override
    public Serializer<SpeedEvent> serializer() {
        return new SpeedEventSerializer();
    }

    @Override
    public Deserializer<SpeedEvent> deserializer() {
        return new SpeedEventDeserializer();
    }

    public static class SpeedEventSerializer implements Serializer<SpeedEvent> {

        @Override
        public byte[] serialize(String topic, SpeedEvent data) {
            try {
                ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
//                            ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
                DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
//                            BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(byteArrayOutputStream));

                dataOutputStream.writeLong(data.timestamp);
                dataOutputStream.writeInt(data.speed);
                dataOutputStream.writeLong(data.vid);
                dataOutputStream.writeInt(data.xWay);
                dataOutputStream.writeInt(data.segment);






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

    public static class SpeedEventDeserializer implements Deserializer<SpeedEvent> {
        @Override
        public SpeedEvent deserialize(String topic, byte[] data) {
            try {
                ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(data);
//                            ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
                DataInputStream dataInputStream = new DataInputStream(byteArrayInputStream);
//                            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(byteArrayInputStream));

                long ts = dataInputStream.readLong();
                int speed = dataInputStream.readInt();
                long vid = dataInputStream.readLong();
                int xWay = dataInputStream.readInt();
                int segment = dataInputStream.readInt();

                dataInputStream.close();
                byteArrayInputStream.close();

                return new SpeedEvent(ts, speed, vid, xWay, segment);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }
    }
}