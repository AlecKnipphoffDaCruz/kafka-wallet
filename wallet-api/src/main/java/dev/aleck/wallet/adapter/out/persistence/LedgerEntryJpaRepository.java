package dev.aleck.wallet.adapter.out.persistence;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;
import org.springframework.data.domain.Limit;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

interface LedgerEntryJpaRepository extends JpaRepository<LedgerEntryEntity, UUID> {

    List<LedgerEntryEntity> findByAccountIdOrderByCreatedAtDesc(UUID accountId, Limit limit);

    /**
     * Reads the generated {@code signed_amount} column, which is not mapped as an attribute.
     * A healthy double-entry book always sums to zero.
     */
    @Query(value = "select coalesce(sum(signed_amount), 0) from ledger_entries", nativeQuery = true)
    BigDecimal signedSum();
}
