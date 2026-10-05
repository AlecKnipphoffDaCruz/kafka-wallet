package dev.aleck.notification;

import dev.aleck.wallet.events.TransferCompleted;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

/**
 * Turns the raw message value into a {@link TransferCompleted}.
 *
 * <p>Parsing happens inside the listener rather than in a value deserialiser on purpose. A
 * deserialiser that throws fails the record before the error handler can see it as a business
 * failure, which makes malformed messages harder to route and easier to lose. Here a bad message
 * is just a {@link NonRetryableEventException} that goes straight to the DLQ.
 */
@Component
class EventParser {

    private final ObjectMapper objectMapper;

    EventParser(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    TransferCompleted parse(String raw) {
        TransferCompleted event;
        try {
            event = objectMapper.readValue(raw, TransferCompleted.class);
        } catch (JacksonException malformed) {
            throw new NonRetryableEventException("message is not valid TransferCompleted JSON", malformed);
        }

        if (event.eventId() == null) {
            // Without an eventId there is no deduplication key, so processing it cannot be made
            // idempotent. Refusing it is safer than guessing.
            throw new NonRetryableEventException("event has no eventId");
        }
        if (!TransferCompleted.TYPE.equals(event.eventType())) {
            throw new NonRetryableEventException("unexpected eventType: " + event.eventType());
        }
        if (event.payload() == null || event.payload().transferId() == null) {
            throw new NonRetryableEventException("event " + event.eventId() + " has no payload");
        }
        return event;
    }
}
