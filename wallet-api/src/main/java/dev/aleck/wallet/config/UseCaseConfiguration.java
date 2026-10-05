package dev.aleck.wallet.config;

import dev.aleck.wallet.application.port.in.AccountQueries;
import dev.aleck.wallet.application.port.in.OpenAccountUseCase;
import dev.aleck.wallet.application.port.in.TransferMoneyUseCase;
import dev.aleck.wallet.application.port.in.TransferQueries;
import dev.aleck.wallet.application.port.out.AccountRepository;
import dev.aleck.wallet.application.port.out.LedgerRepository;
import dev.aleck.wallet.application.port.out.OutboxPort;
import dev.aleck.wallet.application.port.out.TransactionRunner;
import dev.aleck.wallet.application.port.out.TransferRepository;
import dev.aleck.wallet.application.service.AccountQueryService;
import dev.aleck.wallet.application.service.OpenAccountService;
import dev.aleck.wallet.application.service.TransferMoneyService;
import dev.aleck.wallet.application.service.TransferQueryService;
import java.time.Clock;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Wires the use cases by hand.
 *
 * <p>No {@code @Service} anywhere in {@code application} or {@code domain} — those packages compile
 * without Spring on the classpath, which is the actual test of whether the hexagon holds. The cost
 * is this file; the benefit is that the use cases are plain objects you can instantiate in a unit
 * test with three fakes and no context to start.
 */
@Configuration
class UseCaseConfiguration {

    @Bean
    Clock systemClock() {
        // Injected rather than called statically so tests can freeze time.
        return Clock.systemUTC();
    }

    @Bean
    TransferMoneyUseCase transferMoneyUseCase(
            TransactionRunner transactionRunner,
            AccountRepository accounts,
            TransferRepository transfers,
            LedgerRepository ledger,
            OutboxPort outbox,
            Clock clock,
            TransferProperties properties) {

        return new TransferMoneyService(
                transactionRunner,
                accounts,
                transfers,
                ledger,
                outbox,
                clock,
                properties.maxAttempts(),
                properties.backoffBaseMillis());
    }

    @Bean
    OpenAccountUseCase openAccountUseCase(
            TransactionRunner transactionRunner, AccountRepository accounts, Clock clock) {
        return new OpenAccountService(transactionRunner, accounts, clock);
    }

    @Bean
    AccountQueries accountQueries(AccountRepository accounts, LedgerRepository ledger) {
        return new AccountQueryService(accounts, ledger);
    }

    @Bean
    TransferQueries transferQueries(TransferRepository transfers) {
        return new TransferQueryService(transfers);
    }
}
