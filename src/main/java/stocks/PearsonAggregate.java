package stocks;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.*;
import java.util.Objects;

public class PearsonAggregate {
    private Character n1;
    private Character n2;
    private double sumX=0.0;
    private double sumY=0.0;
    private double sumXY=0.0;
    private double sumX2=0.0;
    private double sumY2=0.0;
    private int nCouples = 0;

    public PearsonAggregate(Character n1, Character n2) {
        this.n1 = n1;
        this.n2 = n2;
    }

    public void setUpNames(Character n1, Character n2){
        this.n1 = n1;
        this.n2 = n2;
    }

    @Override
    public String toString() {
        return "PearsonAggregate{" +
                "n1=" + n1 +
                ", n2=" + n2 +
                ", sumX=" + sumX +
                ", sumY=" + sumY +
                ", sumXY=" + sumXY +
                ", sumX2=" + sumX2 +
                ", sumY2=" + sumY2 +
                ", nCouples=" + nCouples +
                '}';
    }

    private PearsonAggregate(Character n1, Character n2, double sumX, double sumY, double sumXY, double sumX2, double sumY2, int nCouples) {
        this.n1 = n1;
        this.n2 = n2;
        this.sumX = sumX;
        this.sumY = sumY;
        this.sumXY = sumXY;
        this.sumX2 = sumX2;
        this.sumY2 = sumY2;
        this.nCouples = nCouples;
    }

    public PearsonAggregate addUp(Pair<Stock, Stock> pair){

        if (getN1()=='Z' && getN2()=='Z'){
            return new PearsonAggregate(pair.getLeft().getName(), pair.getRight().getName(), sumX+=pair.getLeft().getValue(), sumY+=pair.getRight().getValue(),
                    sumXY+=pair.getLeft().getValue()*pair.getRight().getValue(), sumX2+=Math.pow(pair.getLeft().getValue(), 2), sumY2+=Math.pow(pair.getRight().getValue(), 2),
                    ++nCouples);
        } else return new PearsonAggregate(n1, n2, sumX+=pair.getLeft().getValue(), sumY+=pair.getRight().getValue(),
                sumXY+=pair.getLeft().getValue()*pair.getRight().getValue(), sumX2+=Math.pow(pair.getLeft().getValue(), 2), sumY2+=Math.pow(pair.getRight().getValue(), 2),
                ++nCouples);
    }

    public static Serde<PearsonAggregate> serde(){
        return new PearsonSerde();
    }

    public char getN1() {
        return n1;
    }

    public char getN2() {
        return n2;
    }

    private static class PearsonSerde implements Serde<PearsonAggregate>{

        @Override
        public Serializer<PearsonAggregate> serializer() {
            return new Serializer<PearsonAggregate>() {
                @Override
                public byte[] serialize(String topic, PearsonAggregate data) {
                    try {
                        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
//                            ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
                        DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
//                            BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(byteArrayOutputStream));

                        dataOutputStream.writeChar(data.n1);
                        dataOutputStream.writeChar(data.n2);
                        dataOutputStream.writeDouble(data.sumX);
                        dataOutputStream.writeDouble(data.sumX2);
                        dataOutputStream.writeDouble(data.sumXY);
                        dataOutputStream.writeDouble(data.sumY);
                        dataOutputStream.writeDouble(data.sumY2);
                        dataOutputStream.writeInt(data.nCouples);

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
        public Deserializer<PearsonAggregate> deserializer() {
            return new Deserializer<PearsonAggregate>() {
                @Override
                public PearsonAggregate deserialize(String topic, byte[] data) {
                    try {
                        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(data);
//                            ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
                        DataInputStream dataInputStream = new DataInputStream(byteArrayInputStream);
//                            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(byteArrayInputStream));

                        Character n1 = dataInputStream.readChar();
                        Character n2 = dataInputStream.readChar();
                        double sumX = dataInputStream.readDouble();
                        double sumX2 = dataInputStream.readDouble();
                        double sumXY = dataInputStream.readDouble();
                        double sumY = dataInputStream.readDouble();
                        double sumY2 = dataInputStream.readDouble();
                        int nCouples = dataInputStream.readInt();
//                            bufferedReader.close();
                        dataInputStream.close();
                        byteArrayInputStream.close();

                        return new PearsonAggregate(n1, n2, sumX, sumY, sumXY, sumX2, sumY2, nCouples);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    return null;
                }
            };
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PearsonAggregate)) return false;
        PearsonAggregate that = (PearsonAggregate) o;
        return nCouples == that.nCouples && n1.equals(that.n1) && n2.equals(that.n2);
    }

    @Override
    public int hashCode() {
        return Objects.hash(n1, n2, nCouples);
    }

    public static void main(String[] args){

        PearsonAggregate pearsonAggregate = new PearsonAggregate('A', 'G', 12.1, 13.3, 45.6, 23.3, 4.4, 5);
        Serde<PearsonAggregate> pearsonSerde = PearsonAggregate.serde();

        byte[] pearsonAggregateBytes = pearsonSerde.serializer().serialize("csdc", pearsonAggregate);

        System.out.println(pearsonSerde.deserializer().deserialize("csdc", pearsonAggregateBytes));

    }
}
