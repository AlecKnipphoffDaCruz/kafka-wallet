package dev.aleck.wallet.application;

/**
 * Thrown by the persistence adapter when the {@code idempotency_key} unique constraint rejects an
 * insert — i.e. two identical requests raced and both missed the lookup.
 *
 * <p>Not an error for the caller: the use case catches it, re-reads the transfer the winner wrote
 * and returns that instead.
 */
public class DuplicateIdempotencyKeyException extends RuntimeException {

    private final String idempotencyKey;

    public DuplicateIdempotencyKeyException(String idempotencyKey, Throwable cause) {
        super("idempotency key already used: " + idempotencyKey, cause);
        this.idempotencyKey = idempotencyKey;
    }

    public String idempotencyKey() {
        return idempotencyKey;
    }
}
