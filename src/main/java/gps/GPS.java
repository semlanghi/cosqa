package gps;

import org.apache.kafka.common.serialization.Serde;

import java.util.Objects;

public class GPS {
    double x;
    double y;
    long ts;

    public GPS(double x, double y, long ts) {
        this.x = x;
        this.y = y;
        this.ts = ts;
    }


    public String key() {
        return "key";
    }

    public long timestamp() {
        return ts;
    }

    public void addTimestamp(long increment){
        this.ts += increment;
    }

    public double getX() {
        return x;
    }

    public double getY() {
        return y;
    }

    public void dirty() {
        this.x = 11000;
    }


    public static void main(String[] args){
        GPS GPS = new GPS(120030L, 12345L, 5);
        GPS GPS1 = new GPS(120030L, 12345L, 56);
        GPS GPS2 = new GPS(120030L, 12345L, 5);

        Serde<GPS> serde = GPSSerde.instance();

        byte[] GPSByte = serde.serializer().serialize("csbdjc", GPS);
        byte[] GPS1Byte = serde.serializer().serialize("csbdjc", GPS1);
        byte[] GPS2Byte = serde.serializer().serialize("csbdjc", GPS2);

        System.out.println(serde.deserializer().deserialize("csbdjc", GPSByte));
        System.out.println(serde.deserializer().deserialize("csbdjc", GPS1Byte));
        System.out.println(serde.deserializer().deserialize("csbdjc", GPS2Byte));

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof GPS)) return false;
        GPS gps = (GPS) o;
        return Double.compare(gps.x, x) == 0 && Double.compare(gps.y, y) == 0 && ts == gps.ts;
    }

    @Override
    public int hashCode() {
        return Objects.hash(x, y, ts);
    }

    @Override
    public String toString() {
        return "GPS{" +
                "x=" + x +
                ", y=" + y +
                ", ts=" + ts +
                '}';
    }
}
