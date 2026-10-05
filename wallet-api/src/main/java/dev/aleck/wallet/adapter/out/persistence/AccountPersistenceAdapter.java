package dev.aleck.wallet.adapter.out.persistence;

import dev.aleck.wallet.application.AccountNotFoundException;
import dev.aleck.wallet.application.OptimisticConcurrencyException;
import dev.aleck.wallet.application.port.out.AccountRepository;
import dev.aleck.wallet.domain.Account;
import jakarta.persistence.EntityManager;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.stereotype.Repository;

@Repository
class AccountPersistenceAdapter implements AccountRepository {

    private final AccountJpaRepository repository;
    private final EntityManager entityManager;

    AccountPersistenceAdapter(AccountJpaRepository repository, EntityManager entityManager) {
        this.repository = repository;
        this.entityManager = entityManager;
    }

    @Override
    public Optional<Account> findById(UUID id) {
        return repository.findById(id).map(PersistenceMapper::toDomain);
    }

    @Override
    public List<Account> findAllById(List<UUID> ids) {
        return repository.findAllByIdInOrderById(ids).stream()
                .map(PersistenceMapper::toDomain)
                .toList();
    }

    @Override
    public Account insert(Account account) {
        AccountEntity entity = PersistenceMapper.toNewEntity(account);
        // persist(), not save(): the id is assigned by the domain, so Spring Data's "is this new?"
        // guess would turn the insert into a merge and cost a pointless SELECT.
        entityManager.persist(entity);
        entityManager.flush();
        return PersistenceMapper.toDomain(entity);
    }

    @Override
    public void update(Account account) {
        // Already in the persistence context from findAllById, so this is a cache hit, not a query.
        AccountEntity entity = entityManager.find(AccountEntity.class, account.id());
        if (entity == null) {
            throw new AccountNotFoundException(account.id());
        }
        entity.setBalance(account.balance().amount());
        try {
            // Flush here so the versioned UPDATE runs now. Letting it wait until commit would put
            // the failure outside this adapter, where there is no context left to translate it.
            entityManager.flush();
        } catch (RuntimeException failure) {
            if (PersistenceExceptions.isOptimisticLock(failure)) {
                throw new OptimisticConcurrencyException(
                        "account " + account.id() + " changed under us (version "
                                + account.version() + ")", failure);
            }
            throw failure;
        }
    }
}
