package gps;

import annotation.constraint.ConstraintFactory;
import annotation.constraint.PrimaryKeyConstraint;
import annotation.constraint.SpeedConstraint;
import annotation.constraint.StreamingConstraint;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.util.Random;

/**
 * Speed Constraints checks whether
 */
public class PKGPSValueFactory implements ConstraintFactory<ValueAndTimestamp<GPS>> {



    public PKGPSValueFactory() {
    }


    @Override
    public StreamingConstraint<ValueAndTimestamp<GPS>> make(ValueAndTimestamp<GPS> origin) {
        return new PrimaryKeyConstraint<Integer, GPS>(origin) {
            Random random = new Random();
            @Override
            protected Integer getRecordKey(GPS value) {
                return random.nextInt(100000);
            }
        };
    }
}
