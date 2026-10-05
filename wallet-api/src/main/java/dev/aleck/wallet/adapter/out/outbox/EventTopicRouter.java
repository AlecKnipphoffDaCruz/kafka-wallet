package dev.aleck.wallet.adapter.out.outbox;

import dev.aleck.wallet.events.Topics;
import dev.aleck.wallet.events.TransferCompleted;
import java.util.Map;

/** Maps an {@code event_type} stored in the outbox to the topic it belongs on. */
final class EventTopicRouter {

    private static final Map<String, String> ROUTES = Map.of(
            TransferCompleted.TYPE, Topics.TRANSFER_COMPLETED);

    private EventTopicRouter() {
    }

    /**
     * @throws IllegalStateException for an unmapped event type. Deliberately loud: an unroutable
     *         row would otherwise sit at the head of the queue forever, silently blocking every
     *         event behind it. Failing the batch puts it in the logs and the metrics instead.
     */
    static String topicFor(String eventType) {
        String topic = ROUTES.get(eventType);
        if (topic == null) {
            throw new IllegalStateException("no topic mapped for event type '" + eventType + "'");
        }
        return topic;
    }
}
