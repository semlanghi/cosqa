package stocks;

import annotation.constraint.ConstraintFactory;
import annotation.constraint.SpeedConstraint;
import annotation.constraint.StreamingConstraint;
import stocks.Stock;
import org.apache.kafka.streams.state.ValueAndTimestamp;

/**
 * Speed Constraints checks whether
 */
public class SpeedConstraintStockValueFactory implements ConstraintFactory<ValueAndTimestamp<Stock>> {

    private final double maxCoefficient;
    private final double minCoefficient;

    public SpeedConstraintStockValueFactory(double maxCoefficient, double minCoefficient) {
        this.maxCoefficient = maxCoefficient;
        this.minCoefficient = minCoefficient;
    }

    @Override
    public StreamingConstraint<ValueAndTimestamp<Stock>> make(ValueAndTimestamp<Stock> origin) {
        return new SpeedConstraint<>(origin) {
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
