package electricgrid;

import annotation.constraint.ConstraintFactory;
import annotation.constraint.PrimaryKeyConstraint;
import annotation.constraint.StreamingConstraint;
import gps.GPS;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.util.Random;
import java.util.UUID;

/**
 * Speed Constraints checks whether
 */
public class PKElectricGridValueFactory implements ConstraintFactory<ValueAndTimestamp<EGC>> {



    public PKElectricGridValueFactory() {
    }


    @Override
    public String getDescription() {
        return "PK";
    }

    @Override
    public StreamingConstraint<ValueAndTimestamp<EGC>> make(ValueAndTimestamp<EGC> origin) {
        return new PrimaryKeyConstraint<UUID, EGC>(origin) {
            @Override
            protected UUID getRecordKey(EGC value) {
                return value.getUuid();
            }
        };
    }
}
