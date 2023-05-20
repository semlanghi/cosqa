package annotation.constraint;

import org.apache.kafka.streams.state.ValueAndTimestamp;

public record SpeedConstraintLongValueFactory(double maxCoefficient,
                                              double minCoefficient) implements ConstraintFactory<ValueAndTimestamp<Long>> {

    @Override
    public StreamingConstraint<ValueAndTimestamp<Long>> make(ValueAndTimestamp<Long> origin) {
        return new SpeedConstraint<>(origin) {
            @Override
            public double checkConstraint(ValueAndTimestamp<Long> value) {
                if (origin.value() + maxCoefficient * (value.timestamp() - origin.timestamp()) < value.value()) {
                    return Math.abs(value.value() - (origin.value() + maxCoefficient * (value.timestamp() - origin.timestamp())));
                } else if (origin.value() + minCoefficient * (value.timestamp() - origin.timestamp()) > value.value()) {
                    return value.value() - (origin.value() + minCoefficient * (value.timestamp() - origin.timestamp()));
                } else return 0;
            }
        };
    }
}
