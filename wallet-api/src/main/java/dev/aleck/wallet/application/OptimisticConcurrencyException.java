package dev.aleck.wallet.application;

/**
 * Thrown by the persistence adapter when a row was modified by someone else between our read and
 * our write (the {@code version} column did not match).
 *
 * <p>This is the framework-agnostic stand-in for Spring's {@code OptimisticLockingFailureException}:
 * translating at the adapter boundary is what keeps the application layer free of Spring types.
 * It is <em>expected</em> under contention and is retried, not reported.
 */
public class OptimisticConcurrencyException extends RuntimeException {

    public OptimisticConcurrencyException(String message, Throwable cause) {
        super(message, cause);
    }
}
