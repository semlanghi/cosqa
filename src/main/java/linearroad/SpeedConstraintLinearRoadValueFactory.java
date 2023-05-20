package linearroad;

import annotation.constraint.ConstraintFactory;
import annotation.constraint.SpeedConstraint;
import annotation.constraint.StreamingConstraint;
import org.apache.kafka.streams.state.ValueAndTimestamp;

/**
 * Speed Constraints checks whether
 */
public class SpeedConstraintLinearRoadValueFactory implements ConstraintFactory<ValueAndTimestamp<SpeedEvent>> {

    private final double maxCoefficient;
    private final double minCoefficient;

    public SpeedConstraintLinearRoadValueFactory(double maxCoefficient, double minCoefficient) {
        this.maxCoefficient = maxCoefficient;
        this.minCoefficient = minCoefficient;
    }

    @Override
    public StreamingConstraint<ValueAndTimestamp<SpeedEvent>> make(ValueAndTimestamp<SpeedEvent> origin) {
        return new SpeedConstraint<>(origin) {
            @Override
            public double checkConstraint(ValueAndTimestamp<SpeedEvent> value) {
                if (origin.value().speed + maxCoefficient * (value.timestamp() - origin.timestamp()) < value.value().speed) {
                    return Math.abs(value.value().speed - (origin.value().speed + maxCoefficient * (value.timestamp() - origin.timestamp())));
                } else if (origin.value().speed + minCoefficient * (value.timestamp() - origin.timestamp()) > value.value().speed) {
                    return value.value().speed - (origin.value().speed + minCoefficient * (value.timestamp() - origin.timestamp()));
                } else return 0;
            }
        };
    }
}
