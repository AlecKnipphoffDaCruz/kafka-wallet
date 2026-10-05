package dev.aleck.wallet.application;

/**
 * Thrown when a known {@code Idempotency-Key} comes back with a <em>different</em> body.
 *
 * <p>Replaying a key must return the original result; reusing a key for a different transfer is a
 * client bug that would otherwise be silently swallowed, so it gets a 409 instead.
 */
public class IdempotencyKeyReusedException extends RuntimeException {

    public IdempotencyKeyReusedException(String idempotencyKey) {
        super("idempotency key " + idempotencyKey + " was already used for a different request");
    }
}
