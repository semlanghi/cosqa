package stocks;

import annotation.constraint.ConstraintFactory;
import annotation.constraint.SpeedConstraint;
import annotation.constraint.StreamingConstraint;
import org.apache.kafka.streams.state.ValueAndTimestamp;

/**
 * Speed Constraints checks whether
 */
public class SchemaConstraintStockValueFactory implements ConstraintFactory<ValueAndTimestamp<Stock>> {



    public SchemaConstraintStockValueFactory() {
    }

    @Override
    public StreamingConstraint<ValueAndTimestamp<Stock>> make(ValueAndTimestamp<Stock> origin) {
        return new SpeedConstraint<>(origin) {
            @Override
            public double checkConstraint(ValueAndTimestamp<Stock> value) {
                if (value.value().getValue()>100)
                    return -1;
                else return 1;
            }
        };
    }
}
