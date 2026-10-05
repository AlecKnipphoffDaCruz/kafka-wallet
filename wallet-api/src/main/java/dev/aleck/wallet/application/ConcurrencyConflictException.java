package dev.aleck.wallet.application;

/**
 * Thrown when a transfer kept losing the optimistic-lock race until the retry budget ran out.
 * Surfaces as HTTP 409 so the client can decide whether to try again.
 */
public class ConcurrencyConflictException extends RuntimeException {

    public ConcurrencyConflictException(String message, Throwable cause) {
        super(message, cause);
    }
}
