package stocks;

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
public class PKStockValueFactory implements ConstraintFactory<ValueAndTimestamp<Stock>> {

    @Override
    public String getDescription() {
        return "PK";
    }

    public PKStockValueFactory() {
    }

    @Override
    public StreamingConstraint<ValueAndTimestamp<Stock>> make(ValueAndTimestamp<Stock> origin) {
        return new PrimaryKeyConstraint<Integer, Stock>(origin) {
            @Override
            protected Integer getRecordKey(Stock value) {
                return new Random().nextInt(1000000);
            }
        };
    }
}
