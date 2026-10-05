package dev.aleck.notification;

import dev.aleck.wallet.events.TransferCompleted;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Processes one event, exactly once in effect.
 *
 * <p>The ordering inside the transaction is the whole trick:
 *
 * <ol>
 *   <li>claim the event — fails harmlessly if it was already processed;
 *   <li>do the work;
 *   <li>commit both together.
 * </ol>
 *
 * <p>Because the claim and the side effect share a transaction, a failure in step 2 rolls the
 * claim back too, so the retry is free to try again. And because the claim is an insert against a
 * primary key, a redelivery after a successful commit cannot get past step 1. Neither half works
 * without the other: claiming in its own transaction would drop events on failure, and skipping
 * the claim would duplicate them on redelivery.
 *
 * <p>On its own class so {@code @Transactional} goes through the proxy. Calling a transactional
 * method from the listener in the same bean would silently skip the transaction, which is a
 * famously quiet way to break exactly this guarantee.
 */
@Component
class TransferCompletedHandler {

    static final String CONSUMER_NAME = "notification-service";

    private static final Logger log = LoggerFactory.getLogger(TransferCompletedHandler.class);

    private final ProcessedEventStore processedEvents;
    private final NotificationRepository notifications;
    private final FailureInjector failureInjector;
    private final Counter sentCounter;
    private final Counter duplicateCounter;

    TransferCompletedHandler(
            ProcessedEventStore processedEvents,
            NotificationRepository notifications,
            FailureInjector failureInjector,
            MeterRegistry meterRegistry) {
        this.processedEvents = processedEvents;
        this.notifications = notifications;
        this.failureInjector = failureInjector;
        this.sentCounter = Counter.builder("notification.events")
                .tag("outcome", "processed")
                .register(meterRegistry);
        // Expected to be non-zero in a healthy system. A flat zero here usually means the dedup
        // path is never exercised, not that duplicates never arrive.
        this.duplicateCounter = Counter.builder("notification.events")
                .tag("outcome", "duplicate")
                .register(meterRegistry);
    }

    @Transactional
    void handle(TransferCompleted event) {
        if (!processedEvents.claim(event.eventId(), CONSUMER_NAME)) {
            duplicateCounter.increment();
            log.info("event {} was already processed, skipping", event.eventId());
            return;
        }

        failureInjector.failIfArmed(event.eventId().toString());

        TransferCompleted.Payload payload = event.payload();
        notifications.save(new Notification(
                UUID.randomUUID(),
                event.eventId(),
                payload.transferId(),
                payload.fromAccount(),
                "EMAIL",
                "You sent %s %s to account %s".formatted(
                        payload.amount().toPlainString(), payload.currency(), payload.toAccount())));

        sentCounter.increment();
        log.info("notified account {} about transfer {}", payload.fromAccount(), payload.transferId());
    }
}
