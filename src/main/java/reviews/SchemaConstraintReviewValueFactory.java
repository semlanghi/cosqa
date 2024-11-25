package reviews;

import annotation.constraint.ConstraintFactory;
import annotation.constraint.SpeedConstraint;
import annotation.constraint.StreamingConstraint;
import org.apache.kafka.streams.state.ValueAndTimestamp;

/**
 * Speed Constraints checks whether
 */
public class SchemaConstraintReviewValueFactory implements ConstraintFactory<ValueAndTimestamp<Review>> {



    public SchemaConstraintReviewValueFactory() {

    }

    @Override
    public String getDescription() {
        return "Sch";
    }

    @Override
    public StreamingConstraint<ValueAndTimestamp<Review>> make(ValueAndTimestamp<Review> origin) {
        return new SpeedConstraint<>(origin) {
            @Override
            public double checkConstraint(ValueAndTimestamp<Review> value) {
                if (value.value().value>5)
                    return -1;
                else return 1;
            }
        };
    }
}
