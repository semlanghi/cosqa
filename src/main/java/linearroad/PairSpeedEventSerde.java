package linearroad;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.*;

public class PairSpeedEventSerde implements Serde<Pair<SpeedEvent, SpeedEvent>> {
    private Serde<SpeedEvent> speedEventSerde = SpeedEventSerde.instance();

    public PairSpeedEventSerde() {
    }

    public static Serde<Pair<SpeedEvent, SpeedEvent>> instance(){
        return new PairSpeedEventSerde();
    }

    @Override
    public Serializer<Pair<SpeedEvent, SpeedEvent>> serializer() {
        return new Serializer<Pair<SpeedEvent, SpeedEvent>>() {
            @Override
            public byte[] serialize(String topic, Pair<SpeedEvent, SpeedEvent> data) {
                try {
                    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    //                            ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
                    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    //                            BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(byteArrayOutputStream));

                    SpeedEvent left = data.getLeft();
                    SpeedEvent right = data.getRight();


                    dataOutputStream.writeLong(data.getLeft().timestamp);
                    dataOutputStream.writeInt(data.getLeft().speed);
                    dataOutputStream.writeLong(data.getLeft().vid);
                    dataOutputStream.writeInt(data.getLeft().xWay);
                    dataOutputStream.writeInt(data.getLeft().segment);

                    dataOutputStream.writeLong(data.getLeft().timestamp);
                    dataOutputStream.writeInt(data.getLeft().speed);
                    dataOutputStream.writeLong(data.getLeft().vid);
                    dataOutputStream.writeInt(data.getLeft().xWay);
                    dataOutputStream.writeInt(data.getLeft().segment);

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
    public Deserializer<Pair<SpeedEvent, SpeedEvent>> deserializer() {
        return new Deserializer<Pair<SpeedEvent, SpeedEvent>>() {
            @Override
            public Pair<SpeedEvent, SpeedEvent> deserialize(String topic, byte[] data) {
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
                    SpeedEvent left = new SpeedEvent(ts, speed, vid, xWay, segment);

                    ts = dataInputStream.readLong();
                    speed = dataInputStream.readInt();
                    vid = dataInputStream.readLong();
                    xWay = dataInputStream.readInt();
                    segment = dataInputStream.readInt();
                    SpeedEvent right = new SpeedEvent(ts, speed, vid, xWay, segment);


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
