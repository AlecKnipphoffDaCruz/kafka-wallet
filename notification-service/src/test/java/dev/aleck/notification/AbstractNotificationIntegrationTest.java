package dev.aleck.notification;

import dev.aleck.notification.support.SharedContainers;
import dev.aleck.wallet.events.Topics;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

@SpringBootTest(properties = {
        // The endpoint that arms failure injection is off by default; these tests are exactly the
        // place it should be on.
        "notification.failure-injection.enabled=true",
        // Short intervals so exhausting the retry budget takes a moment rather than a minute.
        "notification.consumer.retry.initial-interval=100ms",
        "notification.consumer.retry.max-interval=300ms",
        "notification.consumer.retry.max-attempts=3"
})
abstract class AbstractNotificationIntegrationTest {

    /**
     * A group id unique to this JVM run.
     *
     * <p>With {@code auto-offset-reset=earliest}, a fixed group id would make a second run of the
     * suite resume from offsets committed by the first — or, on a fresh broker, replay every event
     * an earlier test produced. A random group makes each run start from a clean slate.
     */
    private static final String GROUP_ID = "notification-test-" + UUID.randomUUID();

    @Autowired
    protected JdbcTemplate jdbc;

    @Autowired
    protected FailureInjector failureInjector;

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        SharedContainers.registerProperties(registry);
        registry.add("notification.consumer.group-id", () -> GROUP_ID);
    }

    @BeforeEach
    void reset() {
        jdbc.execute("truncate table notifications, processed_events");
        failureInjector.arm(FailureInjector.Mode.NONE, 0);
    }

    protected String transferCompletedJson(UUID eventId, UUID from, UUID to, String amount) {
        return """
                {
                  "eventId": "%s",
                  "eventType": "TransferCompleted",
                  "occurredAt": "%s",
                  "payload": {
                    "transferId": "%s",
                    "fromAccount": "%s",
                    "toAccount": "%s",
                    "amount": "%s",
                    "currency": "BRL"
                  }
                }
                """.formatted(
                eventId, Instant.now(), UUID.randomUUID(), from, to, new BigDecimal(amount));
    }

    protected String topic() {
        return Topics.TRANSFER_COMPLETED;
    }

    protected long notificationCount() {
        return jdbc.queryForObject("select count(*) from notifications", Long.class);
    }

    protected long processedEventCount() {
        return jdbc.queryForObject("select count(*) from processed_events", Long.class);
    }
}
