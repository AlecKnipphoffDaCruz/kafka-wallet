package dev.aleck.wallet.events;

/**
 * Single source of truth for topic names. Keeping them here (instead of scattered
 * {@code @KafkaListener} string literals) means a rename is a compile-time change.
 */
public final class Topics {

    /** Emitted by wallet-api's outbox relay. Key = source accountId. */
    public static final String TRANSFER_COMPLETED = "transfer.completed";

    /** Dead letter queue for {@link #TRANSFER_COMPLETED}. Suffix matches Spring Kafka's default. */
    public static final String TRANSFER_COMPLETED_DLT = "transfer.completed.dlq";

    /** Emitted by fraud-service. Key = accountId under suspicion. */
    public static final String FRAUD_ALERT = "fraud.alert";

    private Topics() {
    }
}
