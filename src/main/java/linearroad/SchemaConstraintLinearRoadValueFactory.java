package linearroad;

import annotation.constraint.ConstraintFactory;
import annotation.constraint.SpeedConstraint;
import annotation.constraint.StreamingConstraint;
import org.apache.kafka.streams.state.ValueAndTimestamp;

/**
 * Speed Constraints checks whether
 */
public class SchemaConstraintLinearRoadValueFactory implements ConstraintFactory<ValueAndTimestamp<SpeedEvent>> {


    public SchemaConstraintLinearRoadValueFactory() {
    }

    @Override
    public StreamingConstraint<ValueAndTimestamp<SpeedEvent>> make(ValueAndTimestamp<SpeedEvent> origin) {
        return new SpeedConstraint<>(origin) {
            @Override
            public double checkConstraint(ValueAndTimestamp<SpeedEvent> value) {
                if (value.value().speed>200)
                    return -1;
                else return 1;
            }
        };
    }
}
