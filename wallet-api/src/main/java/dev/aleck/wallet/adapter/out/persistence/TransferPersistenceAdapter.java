package dev.aleck.wallet.adapter.out.persistence;

import dev.aleck.wallet.application.DuplicateIdempotencyKeyException;
import dev.aleck.wallet.application.port.out.TransferRepository;
import dev.aleck.wallet.domain.Transfer;
import jakarta.persistence.EntityManager;
import java.util.Optional;
import java.util.UUID;
import org.springframework.stereotype.Repository;

@Repository
class TransferPersistenceAdapter implements TransferRepository {

    private final TransferJpaRepository repository;
    private final EntityManager entityManager;

    TransferPersistenceAdapter(TransferJpaRepository repository, EntityManager entityManager) {
        this.repository = repository;
        this.entityManager = entityManager;
    }

    @Override
    public Optional<Transfer> findById(UUID id) {
        return repository.findById(id).map(PersistenceMapper::toDomain);
    }

    @Override
    public Optional<Transfer> findByIdempotencyKey(String idempotencyKey) {
        return repository.findByIdempotencyKey(idempotencyKey).map(PersistenceMapper::toDomain);
    }

    @Override
    public void insert(Transfer transfer) {
        entityManager.persist(PersistenceMapper.toNewEntity(transfer));
        try {
            // Flushing now is what makes the unique constraint a usable control-flow signal:
            // two identical requests race, one gets the row, the other lands here.
            entityManager.flush();
        } catch (RuntimeException failure) {
            if (PersistenceExceptions.isIdempotencyKeyConflict(failure)) {
                throw new DuplicateIdempotencyKeyException(transfer.idempotencyKey(), failure);
            }
            throw failure;
        }
    }
}
