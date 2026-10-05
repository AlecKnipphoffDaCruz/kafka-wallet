package dev.aleck.notification;

import dev.aleck.wallet.events.Topics;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Component;

/**
 * Watches the dead letter topic and does nothing but report.
 *
 * <p>It does <em>not</em> reprocess. Automatic DLQ replay recreates the loop the DLQ exists to
 * break — if a message failed four times with backoff, the cause is usually a bad message or a
 * broken dependency, and both need a human. What this does provide is a loud signal and a metric
 * to alert on, instead of a topic nobody notices until a customer calls.
 *
 * <p>Its own group, so reading the DLQ never disturbs the main consumer's offsets.
 */
@Component
class DeadLetterListener {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterListener.class);

    private final Counter deadLetterCounter;

    DeadLetterListener(MeterRegistry meterRegistry) {
        this.deadLetterCounter = Counter.builder("notification.events")
                .tag("outcome", "dead-lettered")
                .description("Events that exhausted their retries")
                .register(meterRegistry);
    }

    @KafkaListener(
            topics = Topics.TRANSFER_COMPLETED_DLT,
            groupId = "${notification.consumer.group-id}-dlt",
            concurrency = "1")
    void onDeadLetter(ConsumerRecord<String, String> record) {
        deadLetterCounter.increment();

        // Spring Kafka stamps the original coordinates and the stack trace onto the record before
        // publishing it, which is most of what you need to triage without replaying anything.
        log.error("dead letter from {}-{}: key={} reason={} original={}",
                record.topic(),
                record.partition(),
                record.key(),
                header(record, KafkaHeaders.DLT_EXCEPTION_MESSAGE),
                header(record, KafkaHeaders.DLT_ORIGINAL_TOPIC));
    }

    private static String header(ConsumerRecord<String, String> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? "<absent>" : new String(header.value(), StandardCharsets.UTF_8);
    }
}
