package dev.aleck.wallet.domain;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * An account and its balance.
 *
 * <p>The balance is the only mutable state, and it only moves through {@link #debit(Money)} and
 * {@link #credit(Money)} so the "never negative" rule cannot be bypassed.
 *
 * <p>{@link #version()} carries the optimistic-lock version loaded from the database. The domain
 * does not use it, but it has to travel with the aggregate so the persistence adapter can detect a
 * concurrent write on save. Two concurrent transfers on the same account therefore cannot
 * interleave read-modify-write and lose an update: the loser fails and the use case retries.
 */
public final class Account {

    private final UUID id;
    private final String ownerName;
    private Money balance;
    private final long version;
    private final Instant createdAt;

    public Account(UUID id, String ownerName, Money balance, long version, Instant createdAt) {
        this.id = Objects.requireNonNull(id, "id");
        this.ownerName = Objects.requireNonNull(ownerName, "ownerName");
        this.balance = Objects.requireNonNull(balance, "balance");
        this.version = version;
        this.createdAt = Objects.requireNonNull(createdAt, "createdAt");
        if (balance.isNegative()) {
            throw new IllegalArgumentException("balance cannot start negative: " + balance);
        }
    }

    public static Account open(String ownerName, Money initialBalance, Instant at) {
        return new Account(UUID.randomUUID(), ownerName, initialBalance, 0L, at);
    }

    public void debit(Money amount) {
        requirePositive(amount);
        if (balance.isLessThan(amount)) {
            throw new InsufficientFundsException(id, balance, amount);
        }
        balance = balance.minus(amount);
    }

    public void credit(Money amount) {
        requirePositive(amount);
        balance = balance.plus(amount);
    }

    private static void requirePositive(Money amount) {
        if (!amount.isPositive()) {
            throw new IllegalArgumentException("amount must be positive, got " + amount);
        }
    }

    public UUID id() {
        return id;
    }

    public String ownerName() {
        return ownerName;
    }

    public Money balance() {
        return balance;
    }

    public long version() {
        return version;
    }

    public Instant createdAt() {
        return createdAt;
    }
}
