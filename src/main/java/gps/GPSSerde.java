package gps;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.*;

public class GPSSerde implements Serde<GPS> {

    public GPSSerde() {
    }

    public static GPSSerde instance(){
        return new GPSSerde();
    }

    @Override
    public Serializer<GPS> serializer() {
        return new GPSSerializer();
    }

    @Override
    public Deserializer<GPS> deserializer() {
        return new GPSDeserializer();
    }

    public static class GPSSerializer implements Serializer<GPS> {

        @Override
        public byte[] serialize(String topic, GPS data) {
            try {
                ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
//                            ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
                DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
//                            BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(byteArrayOutputStream));

                dataOutputStream.writeLong(data.ts);
                dataOutputStream.writeDouble(data.x);
                dataOutputStream.writeDouble(data.y);





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

    public static class GPSDeserializer implements Deserializer<GPS> {
        @Override
        public GPS deserialize(String topic, byte[] data) {
            try {
                ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(data);
//                            ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
                DataInputStream dataInputStream = new DataInputStream(byteArrayInputStream);
//                            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(byteArrayInputStream));

                long ts = dataInputStream.readLong();
                double x = dataInputStream.readDouble();
                double y = dataInputStream.readDouble();

                dataInputStream.close();
                byteArrayInputStream.close();

                return new GPS(x, y, ts);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }
    }
}