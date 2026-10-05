package dev.aleck.wallet.domain;

import java.util.UUID;

/** A transfer must move money between two <em>different</em> accounts. */
public class SameAccountTransferException extends RuntimeException {

    public SameAccountTransferException(UUID accountId) {
        super("source and destination are the same account: " + accountId);
    }
}
