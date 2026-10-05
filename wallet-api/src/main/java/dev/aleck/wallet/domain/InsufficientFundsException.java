package dev.aleck.wallet.domain;

import java.util.UUID;

/** Raised when an account is asked to part with more money than it holds. */
public class InsufficientFundsException extends RuntimeException {

    private final UUID accountId;

    public InsufficientFundsException(UUID accountId, Money balance, Money requested) {
        super("account " + accountId + " has " + balance + " but " + requested + " was requested");
        this.accountId = accountId;
    }

    public UUID accountId() {
        return accountId;
    }
}
