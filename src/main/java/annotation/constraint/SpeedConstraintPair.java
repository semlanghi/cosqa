package annotation.constraint;

import org.apache.commons.lang3.tuple.Pair;

public abstract class SpeedConstraintPair<V1,V2> implements StreamingConstraint<Pair<V1,V2>> {

    private Pair<V1,V2> origin;

    public SpeedConstraintPair(Pair<V1,V2> origin) {
        this.origin = origin;
    }

    @Override
    public abstract double checkConstraint(Pair<V1,V2> value);

    public Pair<V1,V2> getOrigin() {
        return origin;
    }
}
