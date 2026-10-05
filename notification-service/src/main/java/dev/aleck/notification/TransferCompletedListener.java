package dev.aleck.notification;

import dev.aleck.wallet.events.Topics;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import dev.aleck.wallet.events.TransferCompleted;

/**
 * The thin edge of the consumer: parse, set up logging context, delegate.
 *
 * <p>{@code concurrency} is capped by the topic's partition count — three consumer threads for
 * three partitions. A fourth would start, join the group and then sit idle, because a partition is
 * assigned to at most one consumer in a group. That is also why partitions, not threads, are the
 * unit of consumer scaling.
 */
@Component
class TransferCompletedListener {

    private static final Logger log = LoggerFactory.getLogger(TransferCompletedListener.class);

    private final EventParser parser;
    private final TransferCompletedHandler handler;

    TransferCompletedListener(EventParser parser, TransferCompletedHandler handler) {
        this.parser = parser;
        this.handler = handler;
    }

    @KafkaListener(
            topics = Topics.TRANSFER_COMPLETED,
            groupId = "${notification.consumer.group-id}",
            concurrency = "${notification.consumer.concurrency}")
    void onTransferCompleted(ConsumerRecord<String, String> record) {
        TransferCompleted event = parser.parse(record.value());

        // Stamped into the MDC so every log line from the handler — including retries — can be
        // tied back to one event and one transfer across all three services.
        MDC.put("eventId", event.eventId().toString());
        MDC.put("transferId", event.payload().transferId().toString());
        try {
            log.debug("received event from {}-{} at offset {}",
                    record.topic(), record.partition(), record.offset());
            handler.handle(event);
        } finally {
            MDC.remove("eventId");
            MDC.remove("transferId");
        }
    }
}
