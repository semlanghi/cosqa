package annotation.polynomial;

import org.apache.kafka.streams.kstream.Window;

import java.io.Serializable;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public abstract class Monomial<R> implements Serializable {

    private int degree;
    private int coefficient;
    protected Map<R, Integer> variables;
    private int cardinality;

    public int getDegree() {
        return degree;
    }

    public int getCardinality() {
        return cardinality;
    }

    public Monomial(R variable, int exp) {
        variables = new HashMap<>();
        variables.put(variable, exp);
        degree = exp;
        coefficient = 1;
        cardinality = 1;
    }

    public void simplify(R variable1, R variable2){
        if (variables.isEmpty() || variables.size()<2)
            return;

        if (variables.containsKey(variable1) && variables.containsKey(variable2)){
            Integer integer1 = variables.get(variable1);
            Integer integer2 = variables.get(variable2);
            if (integer1 < integer2){
                variables.put(variable2, integer2 - integer1);
                variables.remove(variable1);
            } else if (integer1 > integer2){
                variables.put(variable1, integer1 - integer2);
                variables.remove(variable2);
            } else {
                variables.remove(variable2);
                variables.remove(variable1);
            }
        }
    }

    public Monomial(int coefficient) {
        this.coefficient = coefficient;
        variables = new HashMap<>();
        cardinality = 0;
    }

    private Monomial(int degree, int coefficient, Map<R, Integer> variables, int cardinality) {
        this.degree = degree;
        this.coefficient = coefficient;
        this.variables = variables;
        this.cardinality = cardinality;
    }

    public Set<R> getVariables() {
        return variables.keySet();
    }

    public Collection<Integer> getExponents() {
        return variables.values().isEmpty() ? Collections.singleton(0) : variables.values();
    }

    public Collection<Integer> getExponents(String prefix) {
        if (variables.values().isEmpty())
            return Collections.singleton(0);

        Set<Integer> collect = variables.keySet().stream()
                .filter(new Predicate<R>() {
                    @Override
                    public boolean test(R r) {
                        return r.toString().startsWith(prefix);
                    }
                }).map(new Function<R, Integer>() {
                    @Override
                    public Integer apply(R r) {
                        return variables.get(r);
                    }
                })
                .collect(Collectors.toSet());

        if (collect.isEmpty())
            return Collections.singleton(0);

        return collect;
    }

    public Monomial() {
        variables = new HashMap<>();
        this.coefficient = 1;
        cardinality = 0;
    }

    public void setDegree(int degree) {
        this.degree = degree;
    }

    public void setCoefficient(int coefficient) {
        this.coefficient = coefficient;
    }

    public void setCardinality(int cardinality) {
        this.cardinality = cardinality;
    }

    public Monomial<R> times(Monomial<R> that){
        Monomial<R> result = emptyMono();
        result.setCoefficient(this.coefficient* that.coefficient);
        result.setDegree(this.degree+that.degree);
        int resCardinality=this.cardinality;
        Map<R, Integer> resVariables = new HashMap<>(this.variables);
        for (R tmp : that.variables.keySet()) {
            resVariables.computeIfPresent(tmp, (v, integer) -> integer + that.variables.get(v));
            if (resVariables.get(tmp) == null){
                resVariables.put(tmp, that.variables.get(tmp));
                resCardinality++;
            }
        }
        result.setVariables(resVariables);
        result.setCardinality(resCardinality);
        return result;
    }

    public void setVariables(Map<R, Integer> variables) {
        this.variables = variables;
    }

    public abstract void slide(Window window);

    public abstract Monomial<R> emptyMono();

    public void times(R variable, int exp){
        this.degree += exp;
        this.variables.computeIfPresent(variable, (v, integer) -> integer + exp);
        this.variables.putIfAbsent(variable, exp);
        this.cardinality++;
    }

    public void timesCoeff(int nwCoeff){
        this.coefficient *= nwCoeff;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(coefficient);
        for (Map.Entry<R, Integer> tmp : variables.entrySet()
             ) {
            if (tmp.getValue()>0)
                sb.append(tmp.getKey().toString()).append("^").append(tmp.getValue());
        }
        return sb.toString();
    }
}
