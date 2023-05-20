package annotation.constraint;

import org.apache.kafka.streams.state.ValueAndTimestamp;

public record SpeedConstraintDoubleValueFactory(double maxCoefficient,
                                                double minCoefficient) implements ConstraintFactory<ValueAndTimestamp<Double>> {

    @Override
    public StreamingConstraint<ValueAndTimestamp<Double>> make(ValueAndTimestamp<Double> origin) {
        return new SpeedConstraint<>(origin) {
            @Override
            public double checkConstraint(ValueAndTimestamp<Double> value) {
                if (origin.value() + maxCoefficient * (value.timestamp() - origin.timestamp()) < value.value()) {
                    return Math.abs(value.value() - (origin.value() + maxCoefficient * (value.timestamp() - origin.timestamp())));
                }
//                else if (origin.value() + minCoefficient * (value.timestamp() - origin.timestamp()) > value.value()) {
//                    return value.value() - (origin.value() + minCoefficient * (value.timestamp() - origin.timestamp()));
//                } else return 0;
                else return 0;
            }
        };
    }
}
