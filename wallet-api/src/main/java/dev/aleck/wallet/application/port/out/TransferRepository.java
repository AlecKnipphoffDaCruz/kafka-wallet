package dev.aleck.wallet.application.port.out;

import dev.aleck.wallet.domain.Transfer;
import java.util.Optional;
import java.util.UUID;

public interface TransferRepository {

    Optional<Transfer> findById(UUID id);

    Optional<Transfer> findByIdempotencyKey(String idempotencyKey);

    /**
     * @throws dev.aleck.wallet.application.DuplicateIdempotencyKeyException if the key is taken
     */
    void insert(Transfer transfer);
}
