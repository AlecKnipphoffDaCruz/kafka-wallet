package dev.aleck.wallet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import dev.aleck.wallet.events.Topics;
import dev.aleck.wallet.events.TransferCompleted;
import dev.aleck.wallet.support.SharedContainers;
import dev.aleck.wallet.support.TestEventConsumer;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import tools.jackson.databind.ObjectMapper;

/**
 * The end-to-end path: HTTP request, database transaction, outbox row, relay, Kafka.
 *
 * <p>This is the test that would catch the mistake the outbox exists to prevent. If the event were
 * published from the service instead of from the committed row, this would still pass — so it also
 * asserts the row's {@code published_at}, which only the relay can set.
 */
class TransferFlowIT extends AbstractWalletIntegrationTest {

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    @DisplayName("a transfer moves money, writes a balanced ledger and reaches the topic")
    void transferReachesKafka() throws Exception {
        UUID alice = openAccount("Alice", "100.00");
        UUID bob = openAccount("Bob", "0.00");

        try (TestEventConsumer events =
                     new TestEventConsumer(SharedContainers.bootstrapServers(), Topics.TRANSFER_COMPLETED)) {

            ResponseEntity<Map> response = transfer("key-flow-1", alice, bob, "40.00");

            assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
            UUID transferId = UUID.fromString((String) response.getBody().get("transferId"));

            assertThat(balanceOf(alice)).isEqualByComparingTo("60.00");
            assertThat(balanceOf(bob)).isEqualByComparingTo("40.00");
            assertThat(ledgerSignedSum()).isEqualByComparingTo(BigDecimal.ZERO);

            // Two entries, one per side, as a unique index guarantees.
            Long entries = jdbc.queryForObject(
                    "select count(*) from ledger_entries where transfer_id = ?", Long.class, transferId);
            assertThat(entries).isEqualTo(2);

            // The relay runs on its own schedule, so the event is eventually — not immediately —
            // on the topic. Everything above was already true when the response returned.
            List<ConsumerRecord<String, String>> received = events.poll(1, Duration.ofSeconds(30));
            assertThat(received).hasSize(1);

            ConsumerRecord<String, String> record = received.getFirst();
            // The key is the source account: that is what pins one account's events to one
            // partition and keeps them ordered.
            assertThat(record.key()).isEqualTo(alice.toString());

            TransferCompleted event = objectMapper.readValue(record.value(), TransferCompleted.class);
            assertThat(event.eventType()).isEqualTo(TransferCompleted.TYPE);
            assertThat(event.eventId()).isNotNull();
            assertThat(event.payload().transferId()).isEqualTo(transferId);
            assertThat(event.payload().fromAccount()).isEqualTo(alice);
            assertThat(event.payload().toAccount()).isEqualTo(bob);
            assertThat(event.payload().amount()).isEqualByComparingTo("40.00");

            // The header duplicates the body's eventId so a consumer can dedupe without parsing.
            assertThat(new String(record.headers().lastHeader("event-id").value()))
                    .isEqualTo(event.eventId().toString());

            // Only the relay sets published_at, so this proves the event came from the outbox row
            // rather than from a direct send inside the use case.
            await().atMost(Duration.ofSeconds(10))
                    .untilAsserted(() -> assertThat(unpublishedOutboxRows()).isZero());

            UUID outboxId = jdbc.queryForObject(
                    "select id from outbox_events where aggregate_id = ?", UUID.class, transferId);
            assertThat(outboxId).isEqualTo(event.eventId());
        }
    }

    @Test
    @DisplayName("an overdraft is rejected and leaves no event behind")
    void insufficientFundsPublishesNothing() {
        UUID alice = openAccount("Alice", "10.00");
        UUID bob = openAccount("Bob", "0.00");

        ResponseEntity<Map> response = transfer("key-flow-2", alice, bob, "999.00");

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNPROCESSABLE_CONTENT);
        assertThat(balanceOf(alice)).isEqualByComparingTo("10.00");

        Long outboxRows = jdbc.queryForObject("select count(*) from outbox_events", Long.class);
        assertThat(outboxRows).isZero();
        Long transfers = jdbc.queryForObject("select count(*) from transfers", Long.class);
        assertThat(transfers).isZero();
    }
}
