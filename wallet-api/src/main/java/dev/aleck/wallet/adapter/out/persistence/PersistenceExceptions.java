package dev.aleck.wallet.adapter.out.persistence;

import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.OptimisticLockingFailureException;

/**
 * Turns persistence-specific failures into the application's own exceptions.
 *
 * <p>This is the whole reason the adapter exists. The same "someone else got there first" event
 * can arrive as {@code jakarta.persistence.OptimisticLockException}, Hibernate's
 * {@code StaleStateException} or Spring's {@code OptimisticLockingFailureException} depending on
 * whether it surfaced on an explicit flush, on commit, or through Spring's translation proxy.
 * The application layer should not have to know about any of them.
 */
final class PersistenceExceptions {

    /** The unique constraint on {@code transfers.idempotency_key}, as named in the migration. */
    private static final String IDEMPOTENCY_CONSTRAINT = "transfers_idempotency_key_unique";

    private PersistenceExceptions() {
    }

    static boolean isOptimisticLock(Throwable failure) {
        for (Throwable t = failure; t != null; t = t.getCause()) {
            if (t instanceof OptimisticLockingFailureException
                    || t instanceof jakarta.persistence.OptimisticLockException
                    || t instanceof org.hibernate.StaleStateException) {
                return true;
            }
            if (t.getCause() == t) {
                break;
            }
        }
        return false;
    }

    static boolean isIdempotencyKeyConflict(Throwable failure) {
        for (Throwable t = failure; t != null; t = t.getCause()) {
            boolean integrityViolation = t instanceof DataIntegrityViolationException
                    || t instanceof jakarta.persistence.EntityExistsException
                    || t instanceof org.hibernate.exception.ConstraintViolationException;
            if (integrityViolation && mentionsIdempotencyConstraint(failure)) {
                return true;
            }
            if (t.getCause() == t) {
                break;
            }
        }
        return false;
    }

    /**
     * Matches on the constraint name rather than on "some unique constraint blew up": a violation
     * of {@code accounts_balance_non_negative} is a bug and must not be mistaken for a replay.
     */
    private static boolean mentionsIdempotencyConstraint(Throwable failure) {
        for (Throwable t = failure; t != null; t = t.getCause()) {
            String message = t.getMessage();
            if (message != null && message.toLowerCase().contains(IDEMPOTENCY_CONSTRAINT)) {
                return true;
            }
            if (t.getCause() == t) {
                break;
            }
        }
        return false;
    }
}
