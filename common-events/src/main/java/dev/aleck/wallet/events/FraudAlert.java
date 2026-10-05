package dev.aleck.wallet.events;

import com.fasterxml.jackson.annotation.JsonFormat;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/** Published by fraud-service when a transfer or a burst of transfers trips a rule. */
public record FraudAlert(
        UUID eventId,
        String eventType,
        Instant occurredAt,
        Payload payload) {

    public static final String TYPE = "FraudAlert";

    public record Payload(
            UUID accountId,
            Reason reason,
            String detail,
            UUID transferId,
            /* A string for the same reason as in TransferCompleted. */
            @JsonFormat(shape = JsonFormat.Shape.STRING) BigDecimal amount,
            int transfersInWindow) {
    }

    public enum Reason {
        /** A single transfer above the configured ceiling. */
        AMOUNT_ABOVE_THRESHOLD,
        /** Too many transfers from the same account inside the configured time window. */
        VELOCITY_IN_WINDOW
    }
}
