package annotation.constraint;

public interface StreamingConstraint<V> {
    double checkConstraint(V value);
    V getOrigin();

    String getDescription();
}
