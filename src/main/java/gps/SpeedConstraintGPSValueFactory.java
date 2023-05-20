package gps;

import annotation.constraint.ConstraintFactory;
import annotation.constraint.SpeedConstraint;
import annotation.constraint.StreamingConstraint;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import reviews.Review;

/**
 * Speed Constraints checks whether
 */
public class SpeedConstraintGPSValueFactory implements ConstraintFactory<ValueAndTimestamp<GPS>> {

    private final double maxCoefficient;
    private final double minCoefficient;

    public SpeedConstraintGPSValueFactory(double maxCoefficient, double minCoefficient) {
        this.maxCoefficient = maxCoefficient;
        this.minCoefficient = minCoefficient;
    }


    @Override
    public StreamingConstraint<ValueAndTimestamp<GPS>> make(ValueAndTimestamp<GPS> origin) {
        return new SpeedConstraint<>(origin) {
            @Override
            public double checkConstraint(ValueAndTimestamp<GPS> value) {
                double distX = 0;
                double distY = 0;
                boolean xZero = false;
                boolean yZero = false;
                if (origin.value().getX() + maxCoefficient * (value.timestamp() - origin.timestamp()) < value.value().getX()) {
                    distX = Math.abs(value.value().getX() - (origin.value().getX() + maxCoefficient * (value.timestamp() - origin.timestamp())));
                } else if (origin.value().getX() + minCoefficient * (value.timestamp() - origin.timestamp()) > value.value().getX()) {
                    distX = value.value().getX() - (origin.value().getX() + minCoefficient * (value.timestamp() - origin.timestamp()));
                } else xZero = true;
                if (origin.value().getY() + maxCoefficient * (value.timestamp() - origin.timestamp()) < value.value().getY()) {
                    distY = Math.abs(value.value().getY() - (origin.value().getY() + maxCoefficient * (value.timestamp() - origin.timestamp())));
                } else if (origin.value().getY() + minCoefficient * (value.timestamp() - origin.timestamp()) > value.value().getY()) {
                    distY = value.value().getY() - (origin.value().getY() + minCoefficient * (value.timestamp() - origin.timestamp()));
                } else yZero = true;

                if (xZero && yZero)
                    return 0;
                else return distX + distY;
            }
        };
    }
}
