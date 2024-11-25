package gps;

import annotation.constraint.ConstraintFactory;
import annotation.constraint.SpeedConstraint;
import annotation.constraint.StreamingConstraint;
import org.apache.kafka.streams.state.ValueAndTimestamp;

/**
 * Speed Constraints checks whether
 */
public class SchemaConstraintGPSValueFactory implements ConstraintFactory<ValueAndTimestamp<GPS>> {



    public SchemaConstraintGPSValueFactory() {
    }

    @Override
    public String getDescription() {
        return "Sch";
    }


    @Override
    public StreamingConstraint<ValueAndTimestamp<GPS>> make(ValueAndTimestamp<GPS> origin) {
        return new SpeedConstraint<>(origin) {
            @Override
            public double checkConstraint(ValueAndTimestamp<GPS> value) {
                if (value.value().getX()>10000)
                    return -1;
                else return 1;
            }
        };
    }
}
