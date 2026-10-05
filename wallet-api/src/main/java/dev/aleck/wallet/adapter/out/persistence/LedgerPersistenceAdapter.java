package dev.aleck.wallet.adapter.out.persistence;

import dev.aleck.wallet.application.port.out.LedgerRepository;
import dev.aleck.wallet.domain.LedgerEntry;
import jakarta.persistence.EntityManager;
import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;
import org.springframework.data.domain.Limit;
import org.springframework.stereotype.Repository;

@Repository
class LedgerPersistenceAdapter implements LedgerRepository {

    private final LedgerEntryJpaRepository repository;
    private final EntityManager entityManager;

    LedgerPersistenceAdapter(LedgerEntryJpaRepository repository, EntityManager entityManager) {
        this.repository = repository;
        this.entityManager = entityManager;
    }

    @Override
    public void insertAll(List<LedgerEntry> entries) {
        entries.stream().map(PersistenceMapper::toNewEntity).forEach(entityManager::persist);
        entityManager.flush();
    }

    @Override
    public List<LedgerEntry> findByAccount(UUID accountId, int limit) {
        return repository.findByAccountIdOrderByCreatedAtDesc(accountId, Limit.of(limit)).stream()
                .map(PersistenceMapper::toDomain)
                .toList();
    }

    @Override
    public BigDecimal signedSumAcrossLedger() {
        return repository.signedSum();
    }
}
