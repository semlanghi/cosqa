package reviews;

import annotation.constraint.ConstraintFactory;
import annotation.constraint.SpeedConstraint;
import annotation.constraint.StreamingConstraint;
import org.apache.kafka.streams.state.ValueAndTimestamp;

/**
 * Speed Constraints checks whether
 */
public class SpeedConstraintReviewValueFactory implements ConstraintFactory<ValueAndTimestamp<Review>> {

    private final double maxCoefficient;
    private final double minCoefficient;

    public SpeedConstraintReviewValueFactory(double maxCoefficient, double minCoefficient) {
        this.maxCoefficient = maxCoefficient;
        this.minCoefficient = minCoefficient;
    }

    @Override
    public StreamingConstraint<ValueAndTimestamp<Review>> make(ValueAndTimestamp<Review> origin) {
        return new SpeedConstraint<>(origin) {
            @Override
            public double checkConstraint(ValueAndTimestamp<Review> value) {
                if (origin.value().value() + maxCoefficient * (value.timestamp() - origin.timestamp()) < value.value().value()) {
                    return Math.abs(value.value().value() - (origin.value().value() + maxCoefficient * (value.timestamp() - origin.timestamp())));
                } else if (origin.value().value() + minCoefficient * (value.timestamp() - origin.timestamp()) > value.value().value()) {
                    return value.value().value() - (origin.value().value() + minCoefficient * (value.timestamp() - origin.timestamp()));
                } else return 0;
            }
        };
    }
}
