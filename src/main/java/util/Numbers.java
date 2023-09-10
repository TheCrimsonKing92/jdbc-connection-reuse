package util;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Objects;

public class Numbers {
    public static BigDecimal toPercentage(final BigDecimal numerator, final BigDecimal denominator, final int scale) {
        Objects.requireNonNull(numerator, "Non-null numerator required");
        Objects.requireNonNull(denominator, "Non-null denominator required");

        return (numerator.multiply(new BigDecimal(100))).divide(denominator, scale, RoundingMode.HALF_UP);
    }

    public static BigDecimal toPercentage(final BigDecimal numerator, final BigDecimal denominator) {
        return toPercentage(numerator, denominator, 4);
    }
}
