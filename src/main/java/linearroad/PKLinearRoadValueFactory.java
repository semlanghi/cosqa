package linearroad;

import annotation.constraint.ConstraintFactory;
import annotation.constraint.PrimaryKeyConstraint;
import annotation.constraint.SpeedConstraint;
import annotation.constraint.StreamingConstraint;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.util.Random;

/**
 * Speed Constraints checks whether
 */
public class PKLinearRoadValueFactory implements ConstraintFactory<ValueAndTimestamp<SpeedEvent>> {


    public PKLinearRoadValueFactory() {
    }

    @Override
    public StreamingConstraint<ValueAndTimestamp<SpeedEvent>> make(ValueAndTimestamp<SpeedEvent> origin) {
        return new PrimaryKeyConstraint<Integer, SpeedEvent>(origin) {
            @Override
            protected Integer getRecordKey(SpeedEvent value) {
                return value.getSegment();
            }
        };
    }
}
