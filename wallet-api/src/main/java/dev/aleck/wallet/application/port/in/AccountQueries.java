package dev.aleck.wallet.application.port.in;

import dev.aleck.wallet.domain.Account;
import dev.aleck.wallet.domain.LedgerEntry;
import java.util.List;
import java.util.UUID;

public interface AccountQueries {

    Account getAccount(UUID accountId);

    List<LedgerEntry> getLedger(UUID accountId, int limit);
}
