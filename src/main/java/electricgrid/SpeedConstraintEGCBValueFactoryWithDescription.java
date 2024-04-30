package electricgrid;

import annotation.constraint.ConstraintFactory;
import annotation.constraint.SpeedConstraint;
import annotation.constraint.StreamingConstraint;
import org.apache.kafka.streams.state.ValueAndTimestamp;

/**
 * Speed Constraints checks whether
 */
public class SpeedConstraintEGCBValueFactoryWithDescription implements ConstraintFactory<ValueAndTimestamp<EGC>> {

    private final double maxCoefficient;
    private final double minCoefficient;
    private final String description;

    public SpeedConstraintEGCBValueFactoryWithDescription(String description, double maxCoefficient, double minCoefficient) {
        this.maxCoefficient = maxCoefficient;
        this.minCoefficient = minCoefficient;
        this.description = description;
    }

    @Override
    public StreamingConstraint<ValueAndTimestamp<EGC>> make(ValueAndTimestamp<EGC> origin) {
        return new SpeedConstraint<>(origin, description) {
            @Override
            public double checkConstraint(ValueAndTimestamp<EGC> value) {
                if (origin.value().getConsB() + maxCoefficient * (value.timestamp() - origin.timestamp()) < value.value().getConsB()) {
                    return Math.abs(value.value().getConsB() - (origin.value().getConsB() + maxCoefficient * (value.timestamp() - origin.timestamp())));
                } else if (origin.value().getConsB() + minCoefficient * (value.timestamp() - origin.timestamp()) > value.value().getConsB()) {
                    return value.value().getConsB() - (origin.value().getConsB() + minCoefficient * (value.timestamp() - origin.timestamp()));
                } else return 0;
            }
        };
    }
}
