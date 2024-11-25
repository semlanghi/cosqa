package stocks;

import annotation.constraint.ConstraintFactory;
import annotation.constraint.SpeedConstraint;
import annotation.constraint.StreamingConstraint;
import org.apache.kafka.streams.state.ValueAndTimestamp;

/**
 * Speed Constraints checks whether
 */
public class SpeedConstraintStockValueFactoryWithDescription implements ConstraintFactory<ValueAndTimestamp<Stock>> {

    private final double maxCoefficient;
    private final double minCoefficient;
    private final String description;

    public SpeedConstraintStockValueFactoryWithDescription(String description, double maxCoefficient, double minCoefficient) {
        this.maxCoefficient = maxCoefficient;
        this.minCoefficient = minCoefficient;
        this.description = description;
    }

    @Override
    public String getDescription() {
        return "SC";
    }

    @Override
    public StreamingConstraint<ValueAndTimestamp<Stock>> make(ValueAndTimestamp<Stock> origin) {
        return new SpeedConstraint<>(origin, description) {
            @Override
            public double checkConstraint(ValueAndTimestamp<Stock> value) {
                if (origin.value().getValue() + maxCoefficient * (value.timestamp() - origin.timestamp()) < value.value().getValue()) {
                    return Math.abs(value.value().getValue() - (origin.value().getValue() + maxCoefficient * (value.timestamp() - origin.timestamp())));
                } else if (origin.value().getValue() + minCoefficient * (value.timestamp() - origin.timestamp()) > value.value().getValue()) {
                    return value.value().getValue() - (origin.value().getValue() + minCoefficient * (value.timestamp() - origin.timestamp()));
                } else return 0;
            }
        };
    }
}
