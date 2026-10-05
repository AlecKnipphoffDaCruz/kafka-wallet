package dev.aleck.wallet.adapter.out.persistence;

import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

interface TransferJpaRepository extends JpaRepository<TransferEntity, UUID> {

    Optional<TransferEntity> findByIdempotencyKey(String idempotencyKey);
}
