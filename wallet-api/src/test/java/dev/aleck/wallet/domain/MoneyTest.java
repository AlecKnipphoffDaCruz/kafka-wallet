package dev.aleck.wallet.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class MoneyTest {

    @Test
    @DisplayName("normalises scale so 10 and 10.00 are the same amount")
    void normalisesScale() {
        assertThat(Money.of("10", "BRL")).isEqualTo(Money.of("10.0000", "BRL"));
        assertThat(Money.of("10", "BRL").amount().scale()).isEqualTo(Money.SCALE);
    }

    @Test
    @DisplayName("refuses to round money away instead of silently losing a fraction of a cent")
    void rejectsOverPreciseAmounts() {
        assertThatExceptionOfType(ArithmeticException.class)
                .isThrownBy(() -> Money.of("10.000001", "BRL"));
    }

    @Test
    @DisplayName("arithmetic is exact, which is the entire reason for BigDecimal")
    void arithmeticIsExact() {
        Money sum = Money.of("0.1", "BRL").plus(Money.of("0.2", "BRL"));

        assertThat(sum).isEqualTo(Money.of("0.3", "BRL"));
        assertThat(sum.amount()).isEqualByComparingTo(new BigDecimal("0.3"));
    }

    @Test
    void comparesAndSubtracts() {
        assertThat(Money.of("5", "BRL").isLessThan(Money.of("10", "BRL"))).isTrue();
        assertThat(Money.of("10", "BRL").minus(Money.of("3", "BRL"))).isEqualTo(Money.of("7", "BRL"));
        assertThat(Money.of("10", "BRL").isGreaterThan(Money.of("10", "BRL"))).isFalse();
        assertThat(Money.zero("BRL").isPositive()).isFalse();
    }

    @Test
    @DisplayName("mixing currencies fails loudly rather than adding the numbers")
    void refusesToMixCurrencies() {
        assertThatThrownBy(() -> Money.of("10", "BRL").plus(Money.of("10", "USD")))
                .isInstanceOf(CurrencyMismatchException.class);
    }

    @Test
    void normalisesCurrencyCaseAndRejectsBadCodes() {
        assertThat(Money.of("1", "brl").currency()).isEqualTo("BRL");
        assertThatThrownBy(() -> Money.of("1", "BRLX")).isInstanceOf(IllegalArgumentException.class);
    }
}
