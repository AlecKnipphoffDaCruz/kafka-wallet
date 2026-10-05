package dev.aleck.notification;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import dev.aleck.notification.support.SharedContainers;
import dev.aleck.notification.support.TestEventConsumer;
import dev.aleck.notification.support.TestEventProducer;
import dev.aleck.wallet.events.Topics;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.KafkaHeaders;

/**
 * What happens when processing fails.
 *
 * <p>Three outcomes, all exercised against a real broker and a real error handler:
 * a transient failure that recovers, a permanent failure that is dead-lettered after its budget
 * runs out, and a malformed message that skips the budget entirely.
 */
class RetryAndDeadLetterIT extends AbstractNotificationIntegrationTest {

    @Test
    @DisplayName("a transient failure is retried and then succeeds exactly once")
    void transientFailureRecovers() {
        UUID from = UUID.randomUUID();
        // Two failures against a budget of three deliveries: the third attempt should get through.
        failureInjector.arm(FailureInjector.Mode.TRANSIENT, 2);

        try (TestEventProducer producer = new TestEventProducer(SharedContainers.bootstrapServers())) {
            producer.send(topic(), from.toString(),
                    transferCompletedJson(UUID.randomUUID(), from, UUID.randomUUID(), "10.00"));

            await().atMost(Duration.ofSeconds(30))
                    .untilAsserted(() -> assertThat(notificationCount()).isEqualTo(1));

            // The two failed attempts rolled back, so the claim row was released each time. One
            // row now, not three: the retry did not leave a trail of half-processed state.
            assertThat(processedEventCount()).isEqualTo(1);
        }
    }

    @Test
    @DisplayName("a permanent failure exhausts its retries and lands on the dead letter topic")
    void permanentFailureIsDeadLettered() {
        UUID eventId = UUID.randomUUID();
        UUID from = UUID.randomUUID();
        failureInjector.arm(FailureInjector.Mode.ALWAYS, 0);

        try (TestEventConsumer deadLetters =
                     new TestEventConsumer(SharedContainers.bootstrapServers(), Topics.TRANSFER_COMPLETED_DLT);
             TestEventProducer producer = new TestEventProducer(SharedContainers.bootstrapServers())) {

            producer.send(topic(), from.toString(),
                    transferCompletedJson(eventId, from, UUID.randomUUID(), "10.00"));

            List<ConsumerRecord<String, String>> dlq = deadLetters.poll(1, Duration.ofSeconds(30));
            assertThat(dlq).hasSize(1);

            ConsumerRecord<String, String> record = dlq.getFirst();
            // The key survives, so a DLQ message can still be traced to an account.
            assertThat(record.key()).isEqualTo(from.toString());
            assertThat(header(record, KafkaHeaders.DLT_ORIGINAL_TOPIC)).isEqualTo(topic());
            assertThat(header(record, KafkaHeaders.DLT_EXCEPTION_MESSAGE))
                    .contains("injected permanent failure");
            // Single-partition DLQ: the recoverer pins everything to partition 0 rather than
            // keeping the original partition number, which may not exist here.
            assertThat(record.partition()).isZero();

            // Nothing was half-done: the last attempt rolled back like the others.
            assertThat(notificationCount()).isZero();
            assertThat(processedEventCount()).isZero();
        }
    }

    @Test
    @DisplayName("a malformed message skips the retries and is dead-lettered immediately")
    void malformedMessageIsNotRetried() {
        try (TestEventConsumer deadLetters =
                     new TestEventConsumer(SharedContainers.bootstrapServers(), Topics.TRANSFER_COMPLETED_DLT);
             TestEventProducer producer = new TestEventProducer(SharedContainers.bootstrapServers())) {

            producer.send(topic(), "some-key", "{ this is not valid json");

            List<ConsumerRecord<String, String>> dlq = deadLetters.poll(1, Duration.ofSeconds(30));
            assertThat(dlq).hasSize(1);
            // The container wraps whatever the listener threw in a ListenerExecutionFailedException,
            // so the real cause lives in the CAUSE_FQCN header — worth knowing when triaging a DLQ.
            assertThat(header(dlq.getFirst(), KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN))
                    .isEqualTo(NonRetryableEventException.class.getName());
            assertThat(header(dlq.getFirst(), KafkaHeaders.DLT_EXCEPTION_MESSAGE))
                    .contains("not valid TransferCompleted JSON");
            assertThat(notificationCount()).isZero();
        }
    }

    @Test
    @DisplayName("an event without an eventId is refused: it cannot be deduplicated")
    void eventWithoutIdIsNotRetried() {
        String noEventId = """
                {
                  "eventType": "TransferCompleted",
                  "occurredAt": "2026-10-05T12:00:00Z",
                  "payload": {
                    "transferId": "%s",
                    "fromAccount": "%s",
                    "toAccount": "%s",
                    "amount": "10.00",
                    "currency": "BRL"
                  }
                }
                """.formatted(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID());

        try (TestEventConsumer deadLetters =
                     new TestEventConsumer(SharedContainers.bootstrapServers(), Topics.TRANSFER_COMPLETED_DLT);
             TestEventProducer producer = new TestEventProducer(SharedContainers.bootstrapServers())) {

            producer.send(topic(), "some-key", noEventId);

            assertThat(deadLetters.poll(1, Duration.ofSeconds(30))).hasSize(1);
            assertThat(notificationCount()).isZero();
        }
    }

    private static String header(ConsumerRecord<String, String> record, String name) {
        var header = record.headers().lastHeader(name);
        assertThat(header).as("header " + name).isNotNull();
        return new String(header.value(), StandardCharsets.UTF_8);
    }
}
