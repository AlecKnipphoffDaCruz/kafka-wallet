package dev.aleck.wallet.application.port.out;

import java.util.UUID;

/**
 * Appends an event to the transactional outbox.
 *
 * <p>The write must land in the <em>same</em> database transaction as the business change. That is
 * the entire point of the pattern: committing the transfer and recording "an event is owed" becomes
 * one atomic act, so there is no window where the money moved but the event vanished (or the
 * reverse). Actually reaching Kafka is a separate, retryable job.
 */
public interface OutboxPort {

    void append(OutboxMessage message);

    /**
     * @param eventId      becomes the outbox row's primary key and the event's deduplication key
     * @param partitionKey the Kafka message key — chosen so related events keep their order
     * @param payload      a contract object from {@code common-events}; the adapter serialises it
     */
    record OutboxMessage(
            UUID eventId,
            String aggregateType,
            UUID aggregateId,
            String eventType,
            String partitionKey,
            Object payload) {
    }
}
