package dev.aleck.wallet.application.port.out;

import java.util.function.Supplier;

/**
 * Runs a unit of work in a database transaction.
 *
 * <p>Exists so the application layer can open a transaction without importing
 * {@code @Transactional}. It also gives the retry loop something to wrap: a failed optimistic lock
 * dooms the current transaction, so retrying has to happen <em>outside</em> it — which is awkward
 * with annotation-driven transactions on the same bean, and trivial here.
 */
public interface TransactionRunner {

    <T> T inTransaction(Supplier<T> work);
}
