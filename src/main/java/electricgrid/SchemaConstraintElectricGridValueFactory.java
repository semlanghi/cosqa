package electricgrid;

import annotation.constraint.ConstraintFactory;
import annotation.constraint.PrimaryKeyConstraint;
import annotation.constraint.SpeedConstraint;
import annotation.constraint.StreamingConstraint;
import gps.GPS;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.util.Random;

/**
 * Speed Constraints checks whether
 */
public class SchemaConstraintElectricGridValueFactory implements ConstraintFactory<ValueAndTimestamp<EGC>> {



    public SchemaConstraintElectricGridValueFactory() {
    }


    @Override
    public String getDescription() {
        return "Sch";
    }

    @Override
    public StreamingConstraint<ValueAndTimestamp<EGC>> make(ValueAndTimestamp<EGC> origin) {
        return new SpeedConstraint<>(origin) {
            @Override
            public double checkConstraint(ValueAndTimestamp<EGC> value) {
                if (value.value().getConsB()<0)
                    return -1;
                else return 1;
            }
        };
    }
}
