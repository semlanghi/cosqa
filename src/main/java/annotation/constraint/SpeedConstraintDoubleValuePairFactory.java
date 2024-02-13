package annotation.constraint;

import org.apache.commons.lang3.tuple.Pair;

public record SpeedConstraintDoubleValuePairFactory(double maxCoefficient,
                                                    double minCoefficient) implements ConstraintFactory<Pair<Double, Long>> {

    @Override
    public StreamingConstraint<Pair<Double, Long>> make(Pair<Double, Long> valueAndTimestamp) {
        return new SpeedConstraintPair<>(valueAndTimestamp) {
            @Override
            public double checkConstraint(Pair<Double, Long> value) {
                if (valueAndTimestamp.getLeft() + maxCoefficient * (value.getRight() - valueAndTimestamp.getRight()) < value.getLeft()) {
                    return Math.abs(value.getLeft() - (valueAndTimestamp.getLeft() + maxCoefficient * (value.getRight() - valueAndTimestamp.getRight())));
                } else if (valueAndTimestamp.getLeft() + minCoefficient * (value.getRight() - valueAndTimestamp.getRight()) > value.getLeft()) {
                    return value.getLeft() - (valueAndTimestamp.getLeft() + minCoefficient * (value.getRight() - valueAndTimestamp.getRight()));
                } else return 0;
            }

            @Override
            public String getDescription() {
                return "SCP";
            }
        };
    }
}
