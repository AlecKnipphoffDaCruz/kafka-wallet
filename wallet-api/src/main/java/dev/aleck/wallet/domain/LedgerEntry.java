package dev.aleck.wallet.domain;

import java.time.Instant;
import java.util.UUID;

/**
 * One immutable line of the ledger. Ledger entries are never updated or deleted — correcting a
 * mistake means posting a compensating entry, which is why this is a record with no setters.
 */
public record LedgerEntry(
        UUID id,
        UUID transferId,
        UUID accountId,
        LedgerEntryType type,
        Money amount,
        Instant createdAt) {

    public LedgerEntry {
        if (!amount.isPositive()) {
            throw new IllegalArgumentException("ledger entries carry a positive amount, got " + amount);
        }
    }

    static LedgerEntry debit(UUID transferId, UUID accountId, Money amount, Instant at) {
        return new LedgerEntry(UUID.randomUUID(), transferId, accountId, LedgerEntryType.DEBIT, amount, at);
    }

    static LedgerEntry credit(UUID transferId, UUID accountId, Money amount, Instant at) {
        return new LedgerEntry(UUID.randomUUID(), transferId, accountId, LedgerEntryType.CREDIT, amount, at);
    }
}
