package dev.aleck.wallet.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TransferTest {

    private static final Instant NOW = Instant.parse("2026-10-05T12:00:00Z");

    @Test
    @DisplayName("posting produces one debit and one credit that cancel out")
    void postingIsDoubleEntry() {
        Account source = Account.open("Ada", Money.of("100", "BRL"), NOW);
        Account destination = Account.open("Linus", Money.of("20", "BRL"), NOW);
        Transfer transfer = initiate(source, destination, "40");

        List<LedgerEntry> entries = transfer.post(source, destination, NOW);

        assertThat(entries).hasSize(2);
        assertThat(entries).extracting(LedgerEntry::type)
                .containsExactly(LedgerEntryType.DEBIT, LedgerEntryType.CREDIT);
        assertThat(entries).allSatisfy(entry ->
                assertThat(entry.transferId()).isEqualTo(transfer.id()));

        // The invariant the ledger exists to guarantee: a transfer creates no money.
        BigDecimal signedSum = entries.stream()
                .map(entry -> entry.type() == LedgerEntryType.DEBIT
                        ? entry.amount().amount().negate()
                        : entry.amount().amount())
                .reduce(BigDecimal.ZERO, BigDecimal::add);
        assertThat(signedSum).isEqualByComparingTo(BigDecimal.ZERO);

        assertThat(source.balance()).isEqualTo(Money.of("60", "BRL"));
        assertThat(destination.balance()).isEqualTo(Money.of("60", "BRL"));
        assertThat(transfer.status()).isEqualTo(TransferStatus.COMPLETED);
    }

    @Test
    @DisplayName("an overdrawn transfer leaves both accounts untouched")
    void failedPostingChangesNothing() {
        Account source = Account.open("Ada", Money.of("10", "BRL"), NOW);
        Account destination = Account.open("Linus", Money.of("20", "BRL"), NOW);
        Transfer transfer = initiate(source, destination, "40");

        assertThatThrownBy(() -> transfer.post(source, destination, NOW))
                .isInstanceOf(InsufficientFundsException.class);

        assertThat(source.balance()).isEqualTo(Money.of("10", "BRL"));
        assertThat(destination.balance()).isEqualTo(Money.of("20", "BRL"));
        assertThat(transfer.status()).isEqualTo(TransferStatus.PENDING);
    }

    @Test
    void cannotBePostedTwice() {
        Account source = Account.open("Ada", Money.of("100", "BRL"), NOW);
        Account destination = Account.open("Linus", Money.of("0", "BRL"), NOW);
        Transfer transfer = initiate(source, destination, "10");

        transfer.post(source, destination, NOW);

        assertThatThrownBy(() -> transfer.post(source, destination, NOW))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void rejectsSelfTransfer() {
        UUID account = UUID.randomUUID();

        assertThatThrownBy(() -> new Transfer(
                UUID.randomUUID(), "key", "fp", account, account,
                Money.of("10", "BRL"), TransferStatus.PENDING, NOW))
                .isInstanceOf(SameAccountTransferException.class);
    }

    private static Transfer initiate(Account source, Account destination, String amount) {
        return Transfer.initiate(
                "key-" + UUID.randomUUID(), "fingerprint",
                source.id(), destination.id(), Money.of(amount, "BRL"), NOW);
    }
}
