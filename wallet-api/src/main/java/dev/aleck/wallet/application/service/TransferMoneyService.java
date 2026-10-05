package dev.aleck.wallet.application.service;

import dev.aleck.wallet.application.AccountNotFoundException;
import dev.aleck.wallet.application.ConcurrencyConflictException;
import dev.aleck.wallet.application.DuplicateIdempotencyKeyException;
import dev.aleck.wallet.application.IdempotencyKeyReusedException;
import dev.aleck.wallet.application.OptimisticConcurrencyException;
import dev.aleck.wallet.application.port.in.TransferMoneyUseCase;
import dev.aleck.wallet.application.port.out.AccountRepository;
import dev.aleck.wallet.application.port.out.LedgerRepository;
import dev.aleck.wallet.application.port.out.OutboxPort;
import dev.aleck.wallet.application.port.out.TransactionRunner;
import dev.aleck.wallet.application.port.out.TransferRepository;
import dev.aleck.wallet.domain.Account;
import dev.aleck.wallet.domain.LedgerEntry;
import dev.aleck.wallet.domain.Money;
import dev.aleck.wallet.domain.SameAccountTransferException;
import dev.aleck.wallet.domain.Transfer;
import dev.aleck.wallet.events.TransferCompleted;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Instant;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * The transfer use case: idempotency in front, one database transaction in the middle, an outbox
 * row on the way out.
 *
 * <p>Three guarantees are worth reading the code for:
 *
 * <ol>
 *   <li><b>Same request, same result.</b> An {@code Idempotency-Key} is looked up first; a hit
 *       returns the original transfer without moving money again. The lookup is not a lock, so two
 *       identical requests can both miss it — the unique constraint settles the race and the loser
 *       re-reads the winner's row.
 *   <li><b>No lost updates.</b> Balances are written with an optimistic version check. Losing that
 *       check is normal under contention, so the whole transaction is retried with jittered
 *       backoff. The retry lives out here precisely because a doomed transaction cannot be reused.
 *   <li><b>The event cannot be lost or invented.</b> Money and outbox row commit together; a
 *       separate relay moves the row to Kafka. See {@link OutboxPort}.
 * </ol>
 */
public class TransferMoneyService implements TransferMoneyUseCase {

    private static final String AGGREGATE_TYPE = "Transfer";

    private final TransactionRunner transactionRunner;
    private final AccountRepository accounts;
    private final TransferRepository transfers;
    private final LedgerRepository ledger;
    private final OutboxPort outbox;
    private final Clock clock;
    private final int maxAttempts;
    private final long backoffBaseMillis;

    public TransferMoneyService(
            TransactionRunner transactionRunner,
            AccountRepository accounts,
            TransferRepository transfers,
            LedgerRepository ledger,
            OutboxPort outbox,
            Clock clock,
            int maxAttempts,
            long backoffBaseMillis) {
        this.transactionRunner = transactionRunner;
        this.accounts = accounts;
        this.transfers = transfers;
        this.ledger = ledger;
        this.outbox = outbox;
        this.clock = clock;
        this.maxAttempts = maxAttempts;
        this.backoffBaseMillis = backoffBaseMillis;
    }

    @Override
    public TransferOutcome transfer(TransferCommand command) {
        if (command.fromAccountId().equals(command.toAccountId())) {
            throw new SameAccountTransferException(command.fromAccountId());
        }

        Money amount = new Money(command.amount(), command.currency());
        String fingerprint = fingerprintOf(command.fromAccountId(), command.toAccountId(), amount);

        Optional<Transfer> alreadyDone = transfers.findByIdempotencyKey(command.idempotencyKey());
        if (alreadyDone.isPresent()) {
            return replay(alreadyDone.get(), command.idempotencyKey(), fingerprint);
        }

        OptimisticConcurrencyException lastFailure = null;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                return transactionRunner.inTransaction(
                        () -> postTransfer(command, amount, fingerprint));
            } catch (OptimisticConcurrencyException contention) {
                lastFailure = contention;
                backoffBeforeRetry(attempt);
            } catch (DuplicateIdempotencyKeyException raceLost) {
                // An identical request committed while we were working. Its transfer is the answer.
                Transfer winner = transfers.findByIdempotencyKey(command.idempotencyKey())
                        .orElseThrow(() -> raceLost);
                return replay(winner, command.idempotencyKey(), fingerprint);
            }
        }

        throw new ConcurrencyConflictException(
                "gave up after " + maxAttempts + " attempts on accounts "
                        + command.fromAccountId() + " -> " + command.toAccountId(),
                lastFailure);
    }

    /** Runs inside the transaction opened by {@link TransactionRunner}. */
    private TransferOutcome postTransfer(TransferCommand command, Money amount, String fingerprint) {
        Instant now = clock.instant();

        List<Account> both = accounts.findAllById(List.of(command.fromAccountId(), command.toAccountId()));
        Account source = require(both, command.fromAccountId());
        Account destination = require(both, command.toAccountId());

        Transfer transfer = Transfer.initiate(
                command.idempotencyKey(), fingerprint, source.id(), destination.id(), amount, now);

        // Mutates both balances and hands back the debit/credit pair; throws before any write if
        // the source cannot cover the amount.
        List<LedgerEntry> entries = transfer.post(source, destination, now);

        transfers.insert(transfer);
        accounts.update(source);
        accounts.update(destination);
        ledger.insertAll(entries);

        outbox.append(new OutboxPort.OutboxMessage(
                UUID.randomUUID(),
                AGGREGATE_TYPE,
                transfer.id(),
                TransferCompleted.TYPE,
                // Keying by the source account keeps one account's transfers on one partition, so
                // consumers see them in the order they happened.
                transfer.fromAccountId().toString(),
                new TransferCompleted.Payload(
                        transfer.id(),
                        transfer.fromAccountId(),
                        transfer.toAccountId(),
                        amount.amount(),
                        amount.currency())));

        return new TransferOutcome(transfer, false);
    }

    private TransferOutcome replay(Transfer stored, String idempotencyKey, String fingerprint) {
        if (!stored.requestFingerprint().equals(fingerprint)) {
            throw new IdempotencyKeyReusedException(idempotencyKey);
        }
        return new TransferOutcome(stored, true);
    }

    private static Account require(List<Account> loaded, UUID id) {
        return loaded.stream()
                .filter(account -> account.id().equals(id))
                .findFirst()
                .orElseThrow(() -> new AccountNotFoundException(id));
    }

    /**
     * Full jitter exponential backoff. The jitter matters more than the growth: without it, two
     * contending transfers retry in lockstep and keep colliding.
     */
    private void backoffBeforeRetry(int attempt) {
        long ceiling = backoffBaseMillis * (1L << (attempt - 1));
        long sleep = ThreadLocalRandom.current().nextLong(1, Math.max(2, ceiling));
        try {
            Thread.sleep(sleep);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new ConcurrencyConflictException("interrupted while retrying transfer", interrupted);
        }
    }

    /**
     * Identifies the <em>content</em> of a request so a replayed {@code Idempotency-Key} can be
     * told apart from a key accidentally reused for a different transfer.
     */
    private static String fingerprintOf(UUID from, UUID to, Money amount) {
        String canonical = String.join("|",
                from.toString(),
                to.toString(),
                amount.amount().toPlainString(),
                amount.currency());
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(canonical.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is required by every JVM", impossible);
        }
    }
}
