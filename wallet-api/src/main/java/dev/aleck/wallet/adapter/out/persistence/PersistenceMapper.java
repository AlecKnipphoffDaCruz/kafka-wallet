package dev.aleck.wallet.adapter.out.persistence;

import dev.aleck.wallet.domain.Account;
import dev.aleck.wallet.domain.LedgerEntry;
import dev.aleck.wallet.domain.Money;
import dev.aleck.wallet.domain.Transfer;

/**
 * Translates between domain objects and JPA entities.
 *
 * <p>The boilerplate here is the price of keeping JPA out of the domain. It buys the ability to
 * change either side independently — and it is the only place that knows both.
 */
final class PersistenceMapper {

    private PersistenceMapper() {
    }

    static Account toDomain(AccountEntity entity) {
        return new Account(
                entity.getId(),
                entity.getOwnerName(),
                new Money(entity.getBalance(), entity.getCurrency()),
                entity.getVersion() == null ? 0L : entity.getVersion(),
                entity.getCreatedAt());
    }

    /**
     * Builds an entity for an <em>insert</em> only. Updates mutate the managed entity instead, so
     * this is never used to round-trip an existing row.
     *
     * <p>The version is deliberately left {@code null}: Hibernate decides whether an instance is
     * transient or detached by looking at the {@code @Version} field, and {@code persist()} on an
     * instance with a version already set fails with "detached entity passed to persist". Null lets
     * it assign the initial version itself.
     */
    static AccountEntity toNewEntity(Account account) {
        return new AccountEntity(
                account.id(),
                account.ownerName(),
                account.balance().amount(),
                account.balance().currency(),
                null,
                account.createdAt());
    }

    static Transfer toDomain(TransferEntity entity) {
        return new Transfer(
                entity.getId(),
                entity.getIdempotencyKey(),
                entity.getRequestFingerprint(),
                entity.getFromAccount(),
                entity.getToAccount(),
                new Money(entity.getAmount(), entity.getCurrency()),
                entity.getStatus(),
                entity.getCreatedAt());
    }

    static TransferEntity toNewEntity(Transfer transfer) {
        return new TransferEntity(
                transfer.id(),
                transfer.idempotencyKey(),
                transfer.requestFingerprint(),
                transfer.fromAccountId(),
                transfer.toAccountId(),
                transfer.amount().amount(),
                transfer.amount().currency(),
                transfer.status(),
                transfer.createdAt());
    }

    static LedgerEntry toDomain(LedgerEntryEntity entity) {
        return new LedgerEntry(
                entity.getId(),
                entity.getTransferId(),
                entity.getAccountId(),
                entity.getEntryType(),
                new Money(entity.getAmount(), entity.getCurrency()),
                entity.getCreatedAt());
    }

    static LedgerEntryEntity toNewEntity(LedgerEntry entry) {
        return new LedgerEntryEntity(
                entry.id(),
                entry.transferId(),
                entry.accountId(),
                entry.type(),
                entry.amount().amount(),
                entry.amount().currency(),
                entry.createdAt());
    }
}
