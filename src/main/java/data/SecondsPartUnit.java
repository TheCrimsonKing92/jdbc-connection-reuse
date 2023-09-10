package data;

public enum SecondsPartUnit {
    NANOS("ns"),
    MILLIS("ms"),
    SECONDS("s");

    SecondsPartUnit(final String unit) {
        this.unit = unit;
    }
    private final String unit;
    public String getUnit() { return unit; }
    @Override
    public String toString() { return getUnit(); }
}
