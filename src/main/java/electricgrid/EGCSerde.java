package electricgrid;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.*;
import java.util.UUID;

public class EGCSerde implements Serde<EGC> {

    public EGCSerde() {
    }

    public static EGCSerde instance(){
        return new EGCSerde();
    }

    @Override
    public Serializer<EGC> serializer() {
        return new EGCSerializer();
    }

    @Override
    public Deserializer<EGC> deserializer() {
        return new EGCDeserializer();
    }

    public static class EGCSerializer implements Serializer<EGC> {

        @Override
        public byte[] serialize(String topic, EGC data) {
            try {
                ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);

                dataOutputStream.writeInt(data.zone);
                dataOutputStream.writeLong(data.consA);
                dataOutputStream.writeLong(data.consB);
                dataOutputStream.writeLong(data.ts);
                dataOutputStream.writeUTF(data.uuid.toString());

                byte[] bytes = byteArrayOutputStream.toByteArray();
                dataOutputStream.close();
                byteArrayOutputStream.close();
                return bytes;
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }
    }

    public static class EGCDeserializer implements Deserializer<EGC> {
        @Override
        public EGC deserialize(String topic, byte[] data) {
            try {
                ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(data);
                DataInputStream dataInputStream = new DataInputStream(byteArrayInputStream);

                int zone = dataInputStream.readInt();
                long consA = dataInputStream.readLong();
                long consB = dataInputStream.readLong();
                long ts = dataInputStream.readLong();
                UUID uuid = UUID.fromString(dataInputStream.readUTF());

                dataInputStream.close();
                byteArrayInputStream.close();

                return new EGC(zone, consA, consB, ts, uuid);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }
    }


    public static void main(String[] args){
        EGC egc1 = new EGC(1, 10, 11, 1, UUID.randomUUID());
        EGC egc2 = new EGC(2, 45, 422, 10, UUID.randomUUID());

        System.out.println(egc1);
        System.out.println(egc2);

        EGCSerde egcSerde = new EGCSerde();

        byte[] seria1 = egcSerde.serializer().serialize("topic", egc1);
        byte[] seria2 = egcSerde.serializer().serialize("topic", egc2);

        EGC dese1 = egcSerde.deserializer().deserialize("topic", seria1);
        EGC dese2 = egcSerde.deserializer().deserialize("topic", seria2);

        System.out.println(dese1);
        System.out.println(dese2);

    }
}