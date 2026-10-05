package dev.aleck.wallet.domain;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Objects;

/**
 * Money is a value object: an amount plus its currency, never a bare {@code double}.
 *
 * <p>Amounts are normalised to {@link #SCALE} decimal places with {@link RoundingMode#UNNECESSARY},
 * which means the constructor <em>throws</em> rather than silently rounding away a fraction of a
 * cent. Over-precise input is rejected at the REST boundary, so reaching that throw means a bug,
 * not bad user input.
 */
public record Money(BigDecimal amount, String currency) implements Comparable<Money> {

    /** Matches {@code NUMERIC(19,4)} in the schema. Four places leaves room for sub-cent rates. */
    public static final int SCALE = 4;

    public Money {
        Objects.requireNonNull(amount, "amount");
        Objects.requireNonNull(currency, "currency");
        if (currency.length() != 3) {
            throw new IllegalArgumentException("currency must be a 3-letter code, got: " + currency);
        }
        currency = currency.toUpperCase();
        amount = amount.setScale(SCALE, RoundingMode.UNNECESSARY);
    }

    public static Money of(String amount, String currency) {
        return new Money(new BigDecimal(amount), currency);
    }

    public static Money zero(String currency) {
        return new Money(BigDecimal.ZERO, currency);
    }

    public Money plus(Money other) {
        requireSameCurrency(other);
        return new Money(amount.add(other.amount), currency);
    }

    public Money minus(Money other) {
        requireSameCurrency(other);
        return new Money(amount.subtract(other.amount), currency);
    }

    public boolean isPositive() {
        return amount.signum() > 0;
    }

    public boolean isNegative() {
        return amount.signum() < 0;
    }

    public boolean isLessThan(Money other) {
        return compareTo(other) < 0;
    }

    public boolean isGreaterThan(Money other) {
        return compareTo(other) > 0;
    }

    @Override
    public int compareTo(Money other) {
        requireSameCurrency(other);
        return amount.compareTo(other.amount);
    }

    private void requireSameCurrency(Money other) {
        if (!currency.equals(other.currency)) {
            throw new CurrencyMismatchException(currency, other.currency);
        }
    }

    @Override
    public String toString() {
        return amount.toPlainString() + " " + currency;
    }
}
