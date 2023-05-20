package linearroad;

import org.apache.kafka.common.serialization.Serde;

import java.util.Objects;

public class SpeedEvent {

    long timestamp;
    int speed;
    long vid;
    int xWay;
    int segment;

    public SpeedEvent(long timestamp, int speed, long vid, int xWay, int segment) {
        this.timestamp = timestamp;
        this.speed = speed;
        this.vid = vid;
        this.xWay = xWay;
        this.segment = segment;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void addTimestamp(long increment) {
        this.timestamp += increment;
    }

    public int getSpeed() {
        return speed;
    }

    public long getVid() {
        return vid;
    }

    public int getxWay() {
        return xWay;
    }

    public int getSegment() {
        return segment;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SpeedEvent)) return false;
        SpeedEvent that = (SpeedEvent) o;
        return timestamp == that.timestamp && speed == that.speed && vid == that.vid;
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, speed, vid);
    }

    @Override
    public String toString() {
        return "SpeedEvent{" +
                "ts=" + timestamp +
                ", speed=" + speed +
                ", vid=" + vid +
                '}';
    }

    public static void main(String[] args){
        SpeedEvent SpeedEvent = new SpeedEvent(120030L, 56, 123452L, 3, 4);
        SpeedEvent SpeedEvent1 = new SpeedEvent(1200301L, 67, 123454L, 4, 6);
        SpeedEvent SpeedEvent2 = new SpeedEvent(120030L, 89, 12345L, 8, 9);

        Serde<SpeedEvent> serde = SpeedEventSerde.instance();

        byte[] SpeedEventByte = serde.serializer().serialize("csbdjc", SpeedEvent);
        byte[] SpeedEvent1Byte = serde.serializer().serialize("csbdjc", SpeedEvent1);
        byte[] SpeedEvent2Byte = serde.serializer().serialize("csbdjc", SpeedEvent2);

        System.out.println(serde.deserializer().deserialize("csbdjc", SpeedEventByte));
        System.out.println(serde.deserializer().deserialize("csbdjc", SpeedEvent1Byte));
        System.out.println(serde.deserializer().deserialize("csbdjc", SpeedEvent2Byte));

    }
}
