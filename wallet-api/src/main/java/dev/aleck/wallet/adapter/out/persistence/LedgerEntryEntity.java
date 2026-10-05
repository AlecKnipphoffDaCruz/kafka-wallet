package dev.aleck.wallet.adapter.out.persistence;

import dev.aleck.wallet.domain.LedgerEntryType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/** Append-only: every column is {@code updatable = false}. */
@Entity
@Table(name = "ledger_entries")
class LedgerEntryEntity {

    @Id
    @Column(name = "id", nullable = false, updatable = false)
    private UUID id;

    @Column(name = "transfer_id", nullable = false, updatable = false)
    private UUID transferId;

    @Column(name = "account_id", nullable = false, updatable = false)
    private UUID accountId;

    @Enumerated(EnumType.STRING)
    @Column(name = "entry_type", nullable = false, updatable = false, length = 6)
    private LedgerEntryType entryType;

    @Column(name = "amount", nullable = false, updatable = false, precision = 19, scale = 4)
    private BigDecimal amount;

    @Column(name = "currency", nullable = false, updatable = false, length = 3)
    private String currency;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    protected LedgerEntryEntity() {
        // required by JPA
    }

    LedgerEntryEntity(
            UUID id,
            UUID transferId,
            UUID accountId,
            LedgerEntryType entryType,
            BigDecimal amount,
            String currency,
            Instant createdAt) {
        this.id = id;
        this.transferId = transferId;
        this.accountId = accountId;
        this.entryType = entryType;
        this.amount = amount;
        this.currency = currency;
        this.createdAt = createdAt;
    }

    UUID getId() {
        return id;
    }

    UUID getTransferId() {
        return transferId;
    }

    UUID getAccountId() {
        return accountId;
    }

    LedgerEntryType getEntryType() {
        return entryType;
    }

    BigDecimal getAmount() {
        return amount;
    }

    String getCurrency() {
        return currency;
    }

    Instant getCreatedAt() {
        return createdAt;
    }
}
