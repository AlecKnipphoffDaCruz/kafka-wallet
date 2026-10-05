package dev.aleck.wallet.application.port.out;

import dev.aleck.wallet.domain.LedgerEntry;
import java.util.List;
import java.util.UUID;

public interface LedgerRepository {

    void insertAll(List<LedgerEntry> entries);

    List<LedgerEntry> findByAccount(UUID accountId, int limit);

    /**
     * Sum of {@code signed_amount} across the whole ledger. A correct double-entry book always
     * returns zero; used by the integrity test rather than by production code.
     */
    java.math.BigDecimal signedSumAcrossLedger();
}
