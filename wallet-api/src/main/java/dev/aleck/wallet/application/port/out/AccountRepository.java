package dev.aleck.wallet.application.port.out;

import dev.aleck.wallet.domain.Account;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface AccountRepository {

    Optional<Account> findById(UUID id);

    /**
     * Loads both sides of a transfer in one round trip.
     *
     * <p>Returned in a deterministic order (sorted by id) rather than the order asked for. That is
     * not cosmetic: with pessimistic locking, two opposite transfers (A→B and B→A) that lock in
     * request order deadlock. A stable global order makes that impossible, and keeping the habit
     * here means switching this adapter to {@code SELECT ... FOR UPDATE} stays safe.
     */
    List<Account> findAllById(List<UUID> ids);

    /**
     * Persists a new account.
     *
     * @return the stored account, with the version assigned by the database
     */
    Account insert(Account account);

    /**
     * Writes back a balance change.
     *
     * @throws dev.aleck.wallet.application.OptimisticConcurrencyException if the row changed since
     *         it was read
     */
    void update(Account account);
}
