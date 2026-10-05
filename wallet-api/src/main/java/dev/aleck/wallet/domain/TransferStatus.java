package dev.aleck.wallet.domain;

/**
 * Lifecycle of a transfer.
 *
 * <p>This project posts both ledger entries inside a single database transaction, so a transfer is
 * born {@code PENDING} and is already {@code COMPLETED} by the time it is written. The state exists
 * because a real system eventually grows an asynchronous step (a clearing house, a fraud hold) and
 * the event contract should not have to change when it does.
 */
public enum TransferStatus {
    PENDING,
    COMPLETED,
    FAILED
}
