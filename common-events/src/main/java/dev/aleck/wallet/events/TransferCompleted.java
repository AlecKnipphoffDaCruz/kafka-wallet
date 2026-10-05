package dev.aleck.wallet.events;

import com.fasterxml.jackson.annotation.JsonFormat;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * Published once a transfer has been committed to the ledger.
 *
 * <p>The envelope fields are deliberately flat and primitive: this contract crosses a service
 * boundary, so it must stay free of domain classes and of any framework type.
 *
 * <p>{@code eventId} is the deduplication key. The outbox relay delivers at-least-once, so a
 * consumer may legitimately see the same {@code eventId} more than once and must treat the
 * second delivery as a no-op.
 */
public record TransferCompleted(
        UUID eventId,
        String eventType,
        Instant occurredAt,
        Payload payload) {

    public static final String TYPE = "TransferCompleted";

    public record Payload(
            UUID transferId,
            UUID fromAccount,
            UUID toAccount,
            /*
             * Encoded as a JSON string, not a number.
             *
             * A JSON number is an IEEE-754 double to most parsers, and JavaScript has no other
             * option — so "150.0000" survives a round trip through a browser while 150.0000 may
             * not. Serialising as a number also drops trailing zeros, which turns a NUMERIC(19,4)
             * amount into something that no longer looks like money.
             */
            @JsonFormat(shape = JsonFormat.Shape.STRING) BigDecimal amount,
            String currency) {
    }
}
