package dev.aleck.notification;

/**
 * A message that will never succeed, no matter how often it is retried — malformed JSON, a missing
 * required field, an event type this consumer does not understand.
 *
 * <p>Separating this from ordinary failures is what keeps the retry budget meaningful. Retrying a
 * broken message four times with backoff just delays the inevitable while blocking its partition;
 * this exception sends it straight to the dead letter topic.
 */
public class NonRetryableEventException extends RuntimeException {

    public NonRetryableEventException(String message, Throwable cause) {
        super(message, cause);
    }

    public NonRetryableEventException(String message) {
        super(message);
    }
}
