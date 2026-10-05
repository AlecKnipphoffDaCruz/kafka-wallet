package dev.aleck.wallet.adapter.out.persistence;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * JPA view of an account. Deliberately separate from {@code domain.Account}: the domain stays free
 * of JPA, and this class is free to carry persistence concerns the domain should not know about —
 * here, the {@link Version} field.
 */
@Entity
@Table(name = "accounts")
class AccountEntity {

    @Id
    @Column(name = "id", nullable = false, updatable = false)
    private UUID id;

    @Column(name = "owner_name", nullable = false, length = 120)
    private String ownerName;

    @Column(name = "balance", nullable = false, precision = 19, scale = 4)
    private BigDecimal balance;

    @Column(name = "currency", nullable = false, length = 3)
    private String currency;

    /**
     * Optimistic lock. Hibernate adds {@code and version = ?} to every UPDATE and raises
     * {@code OptimisticLockingFailureException} when zero rows match.
     */
    @Version
    @Column(name = "version", nullable = false)
    private Long version;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    protected AccountEntity() {
        // required by JPA
    }

    AccountEntity(UUID id, String ownerName, BigDecimal balance, String currency, Long version, Instant createdAt) {
        this.id = id;
        this.ownerName = ownerName;
        this.balance = balance;
        this.currency = currency;
        this.version = version;
        this.createdAt = createdAt;
    }

    UUID getId() {
        return id;
    }

    String getOwnerName() {
        return ownerName;
    }

    BigDecimal getBalance() {
        return balance;
    }

    void setBalance(BigDecimal balance) {
        this.balance = balance;
    }

    String getCurrency() {
        return currency;
    }

    Long getVersion() {
        return version;
    }

    Instant getCreatedAt() {
        return createdAt;
    }
}
