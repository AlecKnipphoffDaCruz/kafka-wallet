package dev.aleck.wallet.adapter.out.persistence;

import dev.aleck.wallet.application.DuplicateIdempotencyKeyException;
import dev.aleck.wallet.application.OptimisticConcurrencyException;
import dev.aleck.wallet.application.port.out.TransactionRunner;
import java.util.function.Supplier;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * The only place that knows how a transaction is started.
 *
 * <p>Uses {@link TransactionTemplate} rather than {@code @Transactional} for two reasons: the
 * application layer stays annotation-free, and the retry loop can wrap whole transactions —
 * something a self-invoked {@code @Transactional} method could not do, since the proxy is bypassed
 * and a rolled-back transaction cannot be reused anyway.
 *
 * <p>Failures that only surface at commit time are translated here, the same way the repository
 * adapters translate failures that surface on flush.
 */
@Component
class SpringTransactionRunner implements TransactionRunner {

    private final TransactionTemplate transactionTemplate;

    SpringTransactionRunner(PlatformTransactionManager transactionManager) {
        this.transactionTemplate = new TransactionTemplate(transactionManager);
        this.transactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRED);
        this.transactionTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
    }

    @Override
    public <T> T inTransaction(Supplier<T> work) {
        try {
            return transactionTemplate.execute(status -> work.get());
        } catch (RuntimeException failure) {
            // The adapters flush explicitly, so these normally surface there with full context.
            // This is the safety net for anything Hibernate defers to commit time.
            if (PersistenceExceptions.isOptimisticLock(failure)) {
                throw new OptimisticConcurrencyException("stale version detected at commit", failure);
            }
            if (PersistenceExceptions.isIdempotencyKeyConflict(failure)) {
                throw new DuplicateIdempotencyKeyException("(detected at commit)", failure);
            }
            throw failure;
        }
    }
}
