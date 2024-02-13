package annotation.constraint;

import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.util.Random;

public abstract class PrimaryKeyConstraint<K,V> implements StreamingConstraint<ValueAndTimestamp<V>>{

    private String description = "PK";
    private ValueAndTimestamp<V> origin;
    private int value1 = new Random().nextInt(10000);
    private int value2 = new Random().nextInt(10000);

    public PrimaryKeyConstraint(ValueAndTimestamp<V> origin) {
        this.origin = origin;
    }

    @Override
    public double checkConstraint(ValueAndTimestamp<V> value){
        if(getRecordKey(value.value()).equals(getRecordKey(origin.value())))
            return -1;
        else return 0;
    }

    @Override
    public ValueAndTimestamp<V> getOrigin() {
        return origin;
    }

    protected abstract K getRecordKey(V value);

    @Override
    public String getDescription() {
        return description+"_"+this.origin.timestamp();
    }
}
