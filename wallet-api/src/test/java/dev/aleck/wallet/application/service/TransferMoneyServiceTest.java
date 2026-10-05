package dev.aleck.wallet.application.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
import dev.aleck.wallet.domain.InsufficientFundsException;
import dev.aleck.wallet.domain.LedgerEntry;
import dev.aleck.wallet.domain.Money;
import dev.aleck.wallet.domain.SameAccountTransferException;
import dev.aleck.wallet.domain.Transfer;
import dev.aleck.wallet.events.TransferCompleted;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the transfer use case, with in-memory fakes instead of a database.
 *
 * <p>This is the payoff for keeping Spring out of the application layer: no context to start, no
 * Mockito, and contention — the hardest case to test — becomes a fake that simply decides to fail.
 */
class TransferMoneyServiceTest {

    private static final Clock FIXED_CLOCK =
            Clock.fixed(Instant.parse("2026-10-05T12:00:00Z"), ZoneOffset.UTC);

    private InMemoryAccounts accounts;
    private InMemoryTransfers transfers;
    private InMemoryLedger ledger;
    private RecordingOutbox outbox;
    private TransferMoneyService service;

    private UUID alice;
    private UUID bob;

    /** How many times a transaction was opened — i.e. how many attempts the retry loop made. */
    private int transactionAttempts;

    @BeforeEach
    void setUp() {
        accounts = new InMemoryAccounts();
        transfers = new InMemoryTransfers();
        ledger = new InMemoryLedger();
        outbox = new RecordingOutbox();
        transactionAttempts = 0;
        service = newService(5);

        alice = accounts.open("Alice", "100.00");
        bob = accounts.open("Bob", "0.00");
    }

    private TransferMoneyService newService(int maxAttempts) {
        return new TransferMoneyService(
                new RollbackAwareTransactionRunner(), accounts, transfers, ledger, outbox,
                FIXED_CLOCK, maxAttempts, 1);
    }

    @Test
    @DisplayName("a transfer moves money, writes the ledger and queues exactly one event")
    void happyPath() {
        var outcome = service.transfer(command("key-1", alice, bob, "40.00"));

        assertThat(outcome.replayed()).isFalse();
        assertThat(accounts.balanceOf(alice)).isEqualTo(Money.of("60.00", "BRL"));
        assertThat(accounts.balanceOf(bob)).isEqualTo(Money.of("40.00", "BRL"));
        assertThat(ledger.entries).hasSize(2);

        assertThat(outbox.messages).hasSize(1);
        OutboxPort.OutboxMessage message = outbox.messages.getFirst();
        assertThat(message.eventType()).isEqualTo(TransferCompleted.TYPE);
        // The key decides the partition, and therefore the ordering guarantee. Source account.
        assertThat(message.partitionKey()).isEqualTo(alice.toString());
        assertThat(message.aggregateId()).isEqualTo(outcome.transfer().id());
    }

    @Test
    @DisplayName("replaying an Idempotency-Key returns the original transfer and moves no money")
    void replayIsIdempotent() {
        var first = service.transfer(command("key-1", alice, bob, "40.00"));
        var second = service.transfer(command("key-1", alice, bob, "40.00"));

        assertThat(second.replayed()).isTrue();
        assertThat(second.transfer().id()).isEqualTo(first.transfer().id());
        assertThat(accounts.balanceOf(alice)).isEqualTo(Money.of("60.00", "BRL"));
        // The second call must not queue a second event, or consumers see the transfer twice.
        assertThat(outbox.messages).hasSize(1);
    }

    @Test
    @DisplayName("scale differences are not different requests: 40 and 40.0000 replay as one")
    void replayIgnoresScale() {
        service.transfer(command("key-1", alice, bob, "40"));
        var replay = service.transfer(command("key-1", alice, bob, "40.0000"));

        assertThat(replay.replayed()).isTrue();
        assertThat(accounts.balanceOf(alice)).isEqualTo(Money.of("60", "BRL"));
    }

    @Test
    @DisplayName("the same key with a different body is a client bug, not a replay")
    void reusingKeyForDifferentRequestIsRejected() {
        service.transfer(command("key-1", alice, bob, "40.00"));

        assertThatThrownBy(() -> service.transfer(command("key-1", alice, bob, "41.00")))
                .isInstanceOf(IdempotencyKeyReusedException.class);
    }

    @Test
    @DisplayName("losing the optimistic lock is retried, not reported")
    void retriesOnContention() {
        accounts.failNextUpdates(2);

        var outcome = service.transfer(command("key-1", alice, bob, "40.00"));

        assertThat(outcome.replayed()).isFalse();
        assertThat(accounts.balanceOf(alice)).isEqualTo(Money.of("60.00", "BRL"));
        // Two attempts lost the race, the third won.
        assertThat(transactionAttempts).isEqualTo(3);
        // Each failed attempt rolled back, so only the winning attempt left an event behind.
        assertThat(outbox.messages).hasSize(1);
    }

    @Test
    @DisplayName("sustained contention eventually becomes a 409 rather than retrying forever")
    void givesUpAfterMaxAttempts() {
        service = newService(3);
        accounts.failNextUpdates(Integer.MAX_VALUE);

        assertThatThrownBy(() -> service.transfer(command("key-1", alice, bob, "40.00")))
                .isInstanceOf(ConcurrencyConflictException.class);

        assertThat(transactionAttempts).isEqualTo(3);
    }

    @Test
    @DisplayName("losing the unique-constraint race returns the winner's transfer")
    void duplicateKeyRaceResolvesToTheWinner() {
        // Simulates the window between the idempotency lookup and the insert: another request with
        // the same key committed first, so our insert is rejected.
        Transfer winner = Transfer.initiate(
                "key-1",
                fingerprintOf(alice, bob, "40.0000"),
                alice, bob, Money.of("40.00", "BRL"), FIXED_CLOCK.instant());
        transfers.rejectNextInsertAndStore(winner);

        var outcome = service.transfer(command("key-1", alice, bob, "40.00"));

        assertThat(outcome.replayed()).isTrue();
        assertThat(outcome.transfer().id()).isEqualTo(winner.id());
        assertThat(outbox.messages).isEmpty();
    }

    @Test
    void rejectsSelfTransferBeforeTouchingTheDatabase() {
        assertThatThrownBy(() -> service.transfer(command("key-1", alice, alice, "10.00")))
                .isInstanceOf(SameAccountTransferException.class);

        // Rejected before a transaction was even opened.
        assertThat(transactionAttempts).isZero();
    }

    @Test
    void rejectsUnknownAccount() {
        assertThatThrownBy(() -> service.transfer(command("key-1", alice, UUID.randomUUID(), "10.00")))
                .isInstanceOf(AccountNotFoundException.class);
    }

    @Test
    @DisplayName("an overdraft queues no event: no money moved, so nothing happened")
    void insufficientFundsQueuesNoEvent() {
        assertThatThrownBy(() -> service.transfer(command("key-1", alice, bob, "1000.00")))
                .isInstanceOf(InsufficientFundsException.class);

        assertThat(outbox.messages).isEmpty();
        assertThat(ledger.entries).isEmpty();
    }

    private static TransferMoneyUseCase.TransferCommand command(
            String key, UUID from, UUID to, String amount) {
        return new TransferMoneyUseCase.TransferCommand(key, from, to, new BigDecimal(amount), "BRL");
    }

    /** Mirrors the service's private fingerprint so the race test can pre-build a matching row. */
    private static String fingerprintOf(UUID from, UUID to, String amount) {
        try {
            byte[] digest = java.security.MessageDigest.getInstance("SHA-256")
                    .digest(String.join("|", from.toString(), to.toString(), amount, "BRL")
                            .getBytes(java.nio.charset.StandardCharsets.UTF_8));
            return java.util.HexFormat.of().formatHex(digest);
        } catch (java.security.NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    /**
     * Runs the work and undoes it on failure.
     *
     * <p>Rolling back is not incidental to this fake — it is the behaviour under test. A retry only
     * works because the failed attempt left nothing behind: no transfer row to collide with the
     * unique constraint, no half-applied balance, no queued event. A fake that merely swallowed the
     * exception would make the retry tests pass for the wrong reason.
     */
    private final class RollbackAwareTransactionRunner implements TransactionRunner {
        @Override
        public <T> T inTransaction(Supplier<T> work) {
            transactionAttempts++;
            List<Runnable> rollback = List.of(
                    accounts.captureRollback(),
                    transfers.captureRollback(),
                    ledger.captureRollback(),
                    outbox.captureRollback());
            try {
                return work.get();
            } catch (RuntimeException failure) {
                rollback.forEach(Runnable::run);
                throw failure;
            }
        }
    }

    private static final class InMemoryAccounts implements AccountRepository {
        private final Map<UUID, Account> stored = new HashMap<>();
        private int failuresLeft;

        UUID open(String owner, String balance) {
            Account account = Account.open(owner, Money.of(balance, "BRL"), FIXED_CLOCK.instant());
            stored.put(account.id(), account);
            return account.id();
        }

        void failNextUpdates(int times) {
            failuresLeft = times;
        }

        /** Restores the rows, but not failuresLeft: that models contention from outside this
         * transaction, which a rollback obviously cannot undo. */
        Runnable captureRollback() {
            Map<UUID, Account> before = new HashMap<>(stored);
            return () -> {
                stored.clear();
                stored.putAll(before);
            };
        }

        Money balanceOf(UUID id) {
            return stored.get(id).balance();
        }

        @Override
        public Optional<Account> findById(UUID id) {
            return Optional.ofNullable(stored.get(id)).map(InMemoryAccounts::copyOf);
        }

        @Override
        public List<Account> findAllById(List<UUID> ids) {
            // Fresh copies, so a rolled-back attempt cannot leak its mutations into the next one —
            // which is exactly what a real transaction rollback guarantees.
            return ids.stream()
                    .map(stored::get)
                    .filter(java.util.Objects::nonNull)
                    .map(InMemoryAccounts::copyOf)
                    .sorted(java.util.Comparator.comparing(Account::id))
                    .toList();
        }

        @Override
        public Account insert(Account account) {
            stored.put(account.id(), account);
            return account;
        }

        @Override
        public void update(Account account) {
            if (failuresLeft > 0) {
                failuresLeft--;
                throw new OptimisticConcurrencyException("simulated contention", null);
            }
            stored.put(account.id(), copyOf(account));
        }

        private static Account copyOf(Account account) {
            return new Account(account.id(), account.ownerName(), account.balance(),
                    account.version(), account.createdAt());
        }
    }

    private static final class InMemoryTransfers implements TransferRepository {
        private final Map<UUID, Transfer> byId = new HashMap<>();
        private final Map<String, Transfer> byKey = new HashMap<>();
        private boolean rejectNextInsert;

        Runnable captureRollback() {
            Map<UUID, Transfer> idsBefore = new HashMap<>(byId);
            Map<String, Transfer> keysBefore = new HashMap<>(byKey);
            return () -> {
                byId.clear();
                byId.putAll(idsBefore);
                byKey.clear();
                byKey.putAll(keysBefore);
            };
        }

        void rejectNextInsertAndStore(Transfer winner) {
            rejectNextInsert = true;
            byId.put(winner.id(), winner);
            byKey.put(winner.idempotencyKey(), winner);
        }

        @Override
        public Optional<Transfer> findById(UUID id) {
            return Optional.ofNullable(byId.get(id));
        }

        @Override
        public Optional<Transfer> findByIdempotencyKey(String idempotencyKey) {
            return rejectNextInsert
                    // Before the insert, the lookup must miss — that is the race being simulated.
                    ? Optional.empty()
                    : Optional.ofNullable(byKey.get(idempotencyKey));
        }

        @Override
        public void insert(Transfer transfer) {
            if (rejectNextInsert) {
                rejectNextInsert = false;
                throw new DuplicateIdempotencyKeyException(transfer.idempotencyKey(), null);
            }
            if (byKey.containsKey(transfer.idempotencyKey())) {
                throw new DuplicateIdempotencyKeyException(transfer.idempotencyKey(), null);
            }
            byId.put(transfer.id(), transfer);
            byKey.put(transfer.idempotencyKey(), transfer);
        }
    }

    private static final class InMemoryLedger implements LedgerRepository {
        private final List<LedgerEntry> entries = new ArrayList<>();

        Runnable captureRollback() {
            List<LedgerEntry> before = List.copyOf(entries);
            return () -> {
                entries.clear();
                entries.addAll(before);
            };
        }

        @Override
        public void insertAll(List<LedgerEntry> newEntries) {
            entries.addAll(newEntries);
        }

        @Override
        public List<LedgerEntry> findByAccount(UUID accountId, int limit) {
            return entries.stream().filter(e -> e.accountId().equals(accountId)).limit(limit).toList();
        }

        @Override
        public BigDecimal signedSumAcrossLedger() {
            return entries.stream()
                    .map(e -> e.type() == dev.aleck.wallet.domain.LedgerEntryType.DEBIT
                            ? e.amount().amount().negate()
                            : e.amount().amount())
                    .reduce(BigDecimal.ZERO, BigDecimal::add);
        }
    }

    private static final class RecordingOutbox implements OutboxPort {
        private final List<OutboxMessage> messages = new ArrayList<>();

        Runnable captureRollback() {
            List<OutboxMessage> before = List.copyOf(messages);
            return () -> {
                messages.clear();
                messages.addAll(before);
            };
        }

        @Override
        public void append(OutboxMessage message) {
            messages.add(message);
        }
    }
}
