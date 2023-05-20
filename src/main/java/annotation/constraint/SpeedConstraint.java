package annotation.constraint;

import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.util.Objects;

public abstract class SpeedConstraint<V> implements StreamingConstraint<ValueAndTimestamp<V>> {

    private ValueAndTimestamp<V> origin;

    public SpeedConstraint(ValueAndTimestamp<V> origin) {
        this.origin = origin;
    }

    @Override
    public abstract double checkConstraint(ValueAndTimestamp<V> value);

    public ValueAndTimestamp<V> getOrigin() {
        return origin;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SpeedConstraint)) return false;
        SpeedConstraint<?> that = (SpeedConstraint<?>) o;
        return Objects.equals(origin, that.origin);
    }

    @Override
    public int hashCode() {
        return Objects.hash(origin);
    }
}
