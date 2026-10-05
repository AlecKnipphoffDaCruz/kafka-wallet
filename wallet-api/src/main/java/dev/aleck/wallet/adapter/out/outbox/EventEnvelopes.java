package dev.aleck.wallet.adapter.out.outbox;

import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

/**
 * Wraps a stored payload in the published envelope.
 *
 * <p>The payload JSON is spliced in as a parsed tree, not re-serialised from an object. The relay
 * therefore does not need to know a single payload type — adding a new event means adding a row,
 * not a class here — and the bytes committed in the transaction are the bytes that reach the topic.
 */
final class EventEnvelopes {

    private EventEnvelopes() {
    }

    static String render(ObjectMapper objectMapper, PendingEvent event) {
        ObjectNode envelope = objectMapper.createObjectNode();
        envelope.put("eventId", event.id().toString());
        envelope.put("eventType", event.eventType());
        envelope.put("occurredAt", event.createdAt().toString());
        envelope.set("payload", objectMapper.readTree(event.payloadJson()));
        return objectMapper.writeValueAsString(envelope);
    }
}
