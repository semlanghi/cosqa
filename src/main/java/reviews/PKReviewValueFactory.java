package reviews;

import annotation.constraint.ConstraintFactory;
import annotation.constraint.PrimaryKeyConstraint;
import annotation.constraint.SpeedConstraint;
import annotation.constraint.StreamingConstraint;
import org.apache.kafka.streams.state.ValueAndTimestamp;

/**
 * Speed Constraints checks whether
 */
public class PKReviewValueFactory implements ConstraintFactory<ValueAndTimestamp<Review>> {



    public PKReviewValueFactory() {

    }

    @Override
    public StreamingConstraint<ValueAndTimestamp<Review>> make(ValueAndTimestamp<Review> origin) {
        return new PrimaryKeyConstraint<Long, Review>(origin) {
            @Override
            protected Long getRecordKey(Review value) {
                return value.userId;
            }
        };
    }
}
