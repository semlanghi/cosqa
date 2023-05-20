package reviews;

import org.apache.kafka.common.serialization.Serde;


import java.util.Date;
import java.util.Objects;

public class Review {
    long userId;
    String title;
    long ts;
    int value;

    public Review(long userId, String title, long ts, int rating) {
        this.userId = userId;
        this.title = title;
        this.ts = ts;
        this.value = rating;
    }

    public Review(long userId, String title, Date date, int rating) {
        this.userId = userId;
        this.title = title;
        this.ts = date.getTime();
        this.value = rating;
    }

    public String key() {
        return title;
    }

    public long timestamp() {
        return ts;
    }

    public void addTimestamp(long ts) {
        this.ts += ts;
    }

    public int value() {
        return value;
    }

    public static void main(String[] args){
        Review review = new Review(120030L, "finalfantasy", 12345L, 5);
        Review review1 = new Review(120030L, "finalfantasy", 12345L, 5);
        Review review2 = new Review(120030L, "finalfantasy", 12345L, 5);

        Serde<Review> serde = ReviewSerde.instance();

        byte[] reviewByte = serde.serializer().serialize("csbdjc", review);
        byte[] review1Byte = serde.serializer().serialize("csbdjc", review1);
        byte[] review2Byte = serde.serializer().serialize("csbdjc", review2);

        System.out.println(serde.deserializer().deserialize("csbdjc", reviewByte));
        System.out.println(serde.deserializer().deserialize("csbdjc", review1Byte));
        System.out.println(serde.deserializer().deserialize("csbdjc", review2Byte));

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Review)) return false;
        Review review = (Review) o;
        return userId == review.userId && ts == review.ts && value == review.value && Objects.equals(title, review.title);
    }

    @Override
    public int hashCode() {
        return Objects.hash(userId, title, ts, value);
    }

    @Override
    public String toString() {
        return "Review{" +
                "userId=" + userId +
                ", title='" + title + '\'' +
                ", ts=" + ts +
                ", value=" + value +
                '}';
    }
}
