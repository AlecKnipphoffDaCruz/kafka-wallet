package dev.aleck.wallet.adapter.out.outbox;

import java.util.UUID;

/**
 * An outbox row the relay has claimed and still owes to Kafka.
 *
 * @param payloadJson the {@code jsonb} column read back as text — the relay forwards it verbatim
 *                    rather than round-tripping it through an object, so the bytes a consumer sees
 *                    are the bytes the producer committed
 */
record PendingEvent(
        UUID id,
        String eventType,
        String partitionKey,
        String payloadJson,
        java.time.Instant createdAt,
        int attempts) {
}
