package data;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;

public class QueryResultAggregate {
    private static final BigDecimal NANOS_TO_MILLIS_FACTOR = new BigDecimal(1_000_000);
    private static final BigDecimal MILLIS_TO_SECONDS_FACTOR = new BigDecimal(1000);
    private List<BigDecimal> results = new ArrayList<>();
    private BigDecimal min = new BigDecimal(Long.MAX_VALUE);
    private BigDecimal max = new BigDecimal(Long.MIN_VALUE);
    private BigDecimal average = BigDecimal.ZERO;
    private BigDecimal total = BigDecimal.ZERO;
    private SecondsPartUnit secondsPart;

    public QueryResultAggregate() {
        this.secondsPart = SecondsPartUnit.NANOS;
    }

    public QueryResultAggregate(final BigDecimal result) {
        this(SecondsPartUnit.NANOS, Collections.singletonList(result));
    }

    public QueryResultAggregate(final Collection<BigDecimal> results) {
        this(SecondsPartUnit.NANOS, results);
    }

    public QueryResultAggregate(final SecondsPartUnit secondsPart) {
        setSecondsPart(secondsPart);
    }

    public QueryResultAggregate(final SecondsPartUnit secondsPart, final Collection<BigDecimal> results) {
        setSecondsPart(secondsPart);
        setResults(results);
    }

    public static QueryResultAggregate reduce(final Collection<QueryResultAggregate> aggregates) {
        QueryResultAggregate result = new QueryResultAggregate(aggregates.stream()
                                                                         .map(QueryResultAggregate::getSecondsPart)
                                                                         .findFirst()
                                                                         .orElseThrow());

        for (final QueryResultAggregate current : aggregates) {
            result = result.addAggregate(current);
        }

        return result;

    }

    public List<BigDecimal> getResults() {
        return results;
    }

    public void setResults(Collection<BigDecimal> results) {
        this.results = new ArrayList<>(results);
        recalculateValues();
    }

    public BigDecimal getTotal() {
        return total;
    }

    public void setTotal(BigDecimal total) {
        this.total = total;
        setAverage();
    }

    public QueryResultAggregate addAggregate(final QueryResultAggregate other) {
        addResults(other.getResults());
        return this;
    }

    public void addResult(final BigDecimal result) {
        this.results.add(result);

        setMin(result);
        setMax(result);
        setTotal(getTotal().add(result));
    }

    public void addResults(final List<BigDecimal> results) {
        this.results.addAll(results);
        recalculateValues();
    }

    public QueryResultAggregate toUnit(final SecondsPartUnit newUnit) {
        if (secondsPart == newUnit) {
            return this;
        }

        if (secondsPart == SecondsPartUnit.NANOS) {
            final BigDecimal divideFactor = newUnit == SecondsPartUnit.MILLIS ? NANOS_TO_MILLIS_FACTOR
                                                                              : MILLIS_TO_SECONDS_FACTOR;
            final List<BigDecimal> newResults = this.results
                                                    .stream()
                                                    .map(result -> result.divide(divideFactor, 4,
                                                            RoundingMode.HALF_UP))
                                                    .toList();
            return new QueryResultAggregate(newUnit, newResults);
        }

        if (secondsPart == SecondsPartUnit.MILLIS) {
            final List<BigDecimal> newResults = this.results
                                                    .stream()
                                                    .map(r -> r.divide(MILLIS_TO_SECONDS_FACTOR, 4,
                                                            RoundingMode.HALF_UP))
                                                    .toList();
            return new QueryResultAggregate(newUnit, newResults);
        }

        throw new RuntimeException("Not allowed to go from " + secondsPart.name() + " to " + newUnit.name());
    }

    public BigDecimal getMin() {
        return min;
    }

    public void setMin(BigDecimal value) {
        if (value == null) {
            return;
        }

        if (this.min == null || value.compareTo(this.min) < 0) {
            this.min = value;
        }
    }

    public BigDecimal getMax() {
        return max;
    }

    public void setMax(BigDecimal value) {
        if (value == null) {
            return;
        }

        if (this.max == null || value.compareTo(this.max) > 0) {
            this.max = value;
        }
    }

    public BigDecimal getAverage() {
        return average;
    }

    public void setAverage() {
        if (this.total == null || this.results == null || this.results.isEmpty()) {
            return;
        }

        this.average = this.total.divide(new BigDecimal(this.results.size()), 4, RoundingMode.HALF_UP);
    }

    public void setAverage(BigDecimal average) {
        this.average = average;
    }

    public SecondsPartUnit getSecondsPart() { return secondsPart; }
    public void setSecondsPart(final SecondsPartUnit secondsPart) { this.secondsPart = secondsPart; }

    private void recalculateValues() {
        for (final BigDecimal result : results) {
            setMin(result);
            setMax(result);
            setTotal(getTotal().add(result));
        }
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", "{", "}")
                    .add(QueryResultAggregate.class.getSimpleName())
                    .add("min: " + getMin())
                    .add("max: " + getMax())
                    .add("average: " + getAverage())
                    .add("total: " + getTotal())
                    .add("unit: " + getSecondsPart())
                    .toString();
    }
}
