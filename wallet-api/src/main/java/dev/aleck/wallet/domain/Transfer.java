package dev.aleck.wallet.domain;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * A transfer between two accounts, and the only place that knows how to turn one into ledger
 * entries.
 *
 * <p>{@link #post(Account, Account)} is the heart of the domain: it mutates both accounts and
 * returns the two postings that must be written with them. It is deliberately all-or-nothing — the
 * caller runs it inside one database transaction, so a thrown exception leaves nothing behind.
 */
public final class Transfer {

    private final UUID id;
    private final String idempotencyKey;
    private final String requestFingerprint;
    private final UUID fromAccountId;
    private final UUID toAccountId;
    private final Money amount;
    private final Instant createdAt;
    private TransferStatus status;

    public Transfer(
            UUID id,
            String idempotencyKey,
            String requestFingerprint,
            UUID fromAccountId,
            UUID toAccountId,
            Money amount,
            TransferStatus status,
            Instant createdAt) {
        this.id = Objects.requireNonNull(id, "id");
        this.idempotencyKey = Objects.requireNonNull(idempotencyKey, "idempotencyKey");
        this.requestFingerprint = Objects.requireNonNull(requestFingerprint, "requestFingerprint");
        this.fromAccountId = Objects.requireNonNull(fromAccountId, "fromAccountId");
        this.toAccountId = Objects.requireNonNull(toAccountId, "toAccountId");
        this.amount = Objects.requireNonNull(amount, "amount");
        this.status = Objects.requireNonNull(status, "status");
        this.createdAt = Objects.requireNonNull(createdAt, "createdAt");

        if (fromAccountId.equals(toAccountId)) {
            throw new SameAccountTransferException(fromAccountId);
        }
        if (!amount.isPositive()) {
            throw new IllegalArgumentException("transfer amount must be positive, got " + amount);
        }
    }

    public static Transfer initiate(
            String idempotencyKey,
            String requestFingerprint,
            UUID fromAccountId,
            UUID toAccountId,
            Money amount,
            Instant at) {
        return new Transfer(
                UUID.randomUUID(),
                idempotencyKey,
                requestFingerprint,
                fromAccountId,
                toAccountId,
                amount,
                TransferStatus.PENDING,
                at);
    }

    /**
     * Moves the money and produces the matching double-entry postings.
     *
     * @return exactly two entries — a debit on the source and a credit on the destination — whose
     *         signed amounts cancel out
     */
    public List<LedgerEntry> post(Account source, Account destination, Instant at) {
        if (status != TransferStatus.PENDING) {
            throw new IllegalStateException("transfer " + id + " is already " + status);
        }
        if (!source.id().equals(fromAccountId) || !destination.id().equals(toAccountId)) {
            throw new IllegalArgumentException("accounts do not match transfer " + id);
        }

        source.debit(amount);
        destination.credit(amount);
        status = TransferStatus.COMPLETED;

        return List.of(
                LedgerEntry.debit(id, fromAccountId, amount, at),
                LedgerEntry.credit(id, toAccountId, amount, at));
    }

    public UUID id() {
        return id;
    }

    public String idempotencyKey() {
        return idempotencyKey;
    }

    public String requestFingerprint() {
        return requestFingerprint;
    }

    public UUID fromAccountId() {
        return fromAccountId;
    }

    public UUID toAccountId() {
        return toAccountId;
    }

    public Money amount() {
        return amount;
    }

    public TransferStatus status() {
        return status;
    }

    public Instant createdAt() {
        return createdAt;
    }
}
