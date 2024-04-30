package electricgrid;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.*;
import java.util.UUID;

public class AvgEGCSerde implements Serde<AvgEGC> {

    public AvgEGCSerde() {
    }

    public static AvgEGCSerde instance(){
        return new AvgEGCSerde();
    }

    @Override
    public Serializer<AvgEGC> serializer() {
        return new AvgEGCSerializer();
    }

    @Override
    public Deserializer<AvgEGC> deserializer() {
        return new AvgEGCDeserializer();
    }

    public static class AvgEGCSerializer implements Serializer<AvgEGC> {

        @Override
        public byte[] serialize(String topic, AvgEGC data) {
            try {
                ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);

                dataOutputStream.writeInt(data.zone);
                dataOutputStream.writeLong(data.sumConsA);
                dataOutputStream.writeLong(data.sumConsB);
                dataOutputStream.writeLong(data.ts);

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

    public static class AvgEGCDeserializer implements Deserializer<AvgEGC> {
        @Override
        public AvgEGC deserialize(String topic, byte[] data) {
            try {
                ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(data);
                DataInputStream dataInputStream = new DataInputStream(byteArrayInputStream);

                int zone = dataInputStream.readInt();
                long consA = dataInputStream.readLong();
                long consB = dataInputStream.readLong();
                long ts = dataInputStream.readLong();

                dataInputStream.close();
                byteArrayInputStream.close();

                return new AvgEGC(zone, consA, consB, ts);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }
    }


    public static void main(String[] args){
        AvgEGC egc1 = new AvgEGC(1, 10, 11, 1);
        AvgEGC egc2 = new AvgEGC(2, 45, 422, 10);

        System.out.println(egc1);
        System.out.println(egc2);

        AvgEGCSerde egcSerde = new AvgEGCSerde();

        byte[] seria1 = egcSerde.serializer().serialize("topic", egc1);
        byte[] seria2 = egcSerde.serializer().serialize("topic", egc2);

        AvgEGC dese1 = egcSerde.deserializer().deserialize("topic", seria1);
        AvgEGC dese2 = egcSerde.deserializer().deserialize("topic", seria2);

        System.out.println(dese1);
        System.out.println(dese2);

    }
}