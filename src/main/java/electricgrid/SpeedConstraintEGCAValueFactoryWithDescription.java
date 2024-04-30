package electricgrid;

import annotation.constraint.ConstraintFactory;
import annotation.constraint.SpeedConstraint;
import annotation.constraint.StreamingConstraint;
import org.apache.kafka.streams.state.ValueAndTimestamp;

/**
 * Speed Constraints checks whether
 */
public class SpeedConstraintEGCAValueFactoryWithDescription implements ConstraintFactory<ValueAndTimestamp<EGC>> {

    private final double maxCoefficient;
    private final double minCoefficient;
    private final String description;

    public SpeedConstraintEGCAValueFactoryWithDescription(String description, double maxCoefficient, double minCoefficient) {
        this.maxCoefficient = maxCoefficient;
        this.minCoefficient = minCoefficient;
        this.description = description;
    }

    @Override
    public StreamingConstraint<ValueAndTimestamp<EGC>> make(ValueAndTimestamp<EGC> origin) {
        return new SpeedConstraint<>(origin, description) {
            @Override
            public double checkConstraint(ValueAndTimestamp<EGC> value) {
                if (origin.value().getConsA() + maxCoefficient * (value.timestamp() - origin.timestamp()) < value.value().getConsA()) {
                    return Math.abs(value.value().getConsA() - (origin.value().getConsA() + maxCoefficient * (value.timestamp() - origin.timestamp())));
                } else if (origin.value().getConsA() + minCoefficient * (value.timestamp() - origin.timestamp()) > value.value().getConsA()) {
                    return value.value().getConsA() - (origin.value().getConsA() + minCoefficient * (value.timestamp() - origin.timestamp()));
                } else return 0;
            }
        };
    }
}
