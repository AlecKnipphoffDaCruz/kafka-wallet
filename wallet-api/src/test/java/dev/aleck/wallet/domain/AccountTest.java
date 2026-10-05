package dev.aleck.wallet.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class AccountTest {

    private static final Instant NOW = Instant.parse("2026-10-05T12:00:00Z");

    @Test
    void debitsAndCredits() {
        Account account = Account.open("Ada", Money.of("100", "BRL"), NOW);

        account.debit(Money.of("30", "BRL"));
        account.credit(Money.of("5", "BRL"));

        assertThat(account.balance()).isEqualTo(Money.of("75", "BRL"));
    }

    @Test
    @DisplayName("a balance can reach zero but never go below it")
    void refusesToOverdraw() {
        Account account = Account.open("Ada", Money.of("100", "BRL"), NOW);

        account.debit(Money.of("100", "BRL"));
        assertThat(account.balance()).isEqualTo(Money.zero("BRL"));

        assertThatThrownBy(() -> account.debit(Money.of("0.0001", "BRL")))
                .isInstanceOf(InsufficientFundsException.class);
    }

    @Test
    void refusesNonPositiveMovements() {
        Account account = Account.open("Ada", Money.of("100", "BRL"), NOW);

        assertThatThrownBy(() -> account.debit(Money.zero("BRL")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> account.credit(Money.of("-1", "BRL")))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
