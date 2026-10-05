package dev.aleck.wallet.application.service;

import dev.aleck.wallet.application.port.in.OpenAccountUseCase;
import dev.aleck.wallet.application.port.out.AccountRepository;
import dev.aleck.wallet.application.port.out.TransactionRunner;
import dev.aleck.wallet.domain.Account;
import dev.aleck.wallet.domain.Money;
import java.time.Clock;

public class OpenAccountService implements OpenAccountUseCase {

    private final TransactionRunner transactionRunner;
    private final AccountRepository accounts;
    private final Clock clock;

    public OpenAccountService(TransactionRunner transactionRunner, AccountRepository accounts, Clock clock) {
        this.transactionRunner = transactionRunner;
        this.accounts = accounts;
        this.clock = clock;
    }

    @Override
    public Account open(OpenAccountCommand command) {
        Account account = Account.open(
                command.ownerName(),
                new Money(command.initialBalance(), command.currency()),
                clock.instant());

        return transactionRunner.inTransaction(() -> accounts.insert(account));
    }
}
