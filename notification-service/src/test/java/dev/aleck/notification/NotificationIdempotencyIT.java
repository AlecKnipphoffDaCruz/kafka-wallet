package dev.aleck.notification;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import dev.aleck.notification.support.SharedContainers;
import dev.aleck.notification.support.TestEventProducer;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The consumer-side half of the delivery guarantee.
 *
 * <p>The relay is at-least-once, so the only thing standing between a duplicate event and a
 * duplicate notification is this. These tests publish the duplicate on purpose, which is the only
 * honest way to know the dedup works — waiting for one to happen naturally would mean waiting for
 * a crash at exactly the wrong moment.
 */
class NotificationIdempotencyIT extends AbstractNotificationIntegrationTest {

    @Test
    @DisplayName("an event is processed once")
    void processesEventOnce() {
        UUID eventId = UUID.randomUUID();
        UUID from = UUID.randomUUID();

        try (TestEventProducer producer = new TestEventProducer(SharedContainers.bootstrapServers())) {
            producer.send(topic(), from.toString(),
                    transferCompletedJson(eventId, from, UUID.randomUUID(), "150.00"));

            await().atMost(Duration.ofSeconds(30))
                    .untilAsserted(() -> assertThat(notificationCount()).isEqualTo(1));
        }
    }

    @Test
    @DisplayName("the same eventId delivered three times still produces one notification")
    void duplicateDeliveriesAreIgnored() {
        UUID eventId = UUID.randomUUID();
        UUID from = UUID.randomUUID();
        String event = transferCompletedJson(eventId, from, UUID.randomUUID(), "150.00");

        try (TestEventProducer producer = new TestEventProducer(SharedContainers.bootstrapServers())) {
            producer.send(topic(), from.toString(), event);
            producer.send(topic(), from.toString(), event);
            producer.send(topic(), from.toString(), event);

            await().atMost(Duration.ofSeconds(30))
                    .untilAsserted(() -> assertThat(processedEventCount()).isEqualTo(1));

            // Give the two redeliveries time to be wrongly processed, if the dedup were broken.
            // Asserting immediately could pass simply because they had not arrived yet.
            await().during(Duration.ofSeconds(2))
                    .atMost(Duration.ofSeconds(10))
                    .untilAsserted(() -> assertThat(notificationCount()).isEqualTo(1));
        }
    }

    @Test
    @DisplayName("two different events are two notifications: dedup is per event, not per account")
    void differentEventsAreBothProcessed() {
        UUID from = UUID.randomUUID();

        try (TestEventProducer producer = new TestEventProducer(SharedContainers.bootstrapServers())) {
            producer.send(topic(), from.toString(),
                    transferCompletedJson(UUID.randomUUID(), from, UUID.randomUUID(), "10.00"));
            producer.send(topic(), from.toString(),
                    transferCompletedJson(UUID.randomUUID(), from, UUID.randomUUID(), "20.00"));

            await().atMost(Duration.ofSeconds(30))
                    .untilAsserted(() -> assertThat(notificationCount()).isEqualTo(2));
        }
    }
}
