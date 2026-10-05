package dev.aleck.wallet.application.service;

import dev.aleck.wallet.application.AccountNotFoundException;
import dev.aleck.wallet.application.port.in.AccountQueries;
import dev.aleck.wallet.application.port.out.AccountRepository;
import dev.aleck.wallet.application.port.out.LedgerRepository;
import dev.aleck.wallet.domain.Account;
import dev.aleck.wallet.domain.LedgerEntry;
import java.util.List;
import java.util.UUID;

public class AccountQueryService implements AccountQueries {

    private final AccountRepository accounts;
    private final LedgerRepository ledger;

    public AccountQueryService(AccountRepository accounts, LedgerRepository ledger) {
        this.accounts = accounts;
        this.ledger = ledger;
    }

    @Override
    public Account getAccount(UUID accountId) {
        return accounts.findById(accountId).orElseThrow(() -> new AccountNotFoundException(accountId));
    }

    @Override
    public List<LedgerEntry> getLedger(UUID accountId, int limit) {
        // Checked first so an unknown account is a 404 rather than an empty list.
        getAccount(accountId);
        return ledger.findByAccount(accountId, limit);
    }
}
