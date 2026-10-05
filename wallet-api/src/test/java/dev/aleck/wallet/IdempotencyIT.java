package dev.aleck.wallet;

import static org.assertj.core.api.Assertions.assertThat;

import dev.aleck.wallet.events.Topics;
import dev.aleck.wallet.support.SharedContainers;
import dev.aleck.wallet.support.TestEventConsumer;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

/** The API-level idempotency guarantee, including the race the unique constraint settles. */
class IdempotencyIT extends AbstractWalletIntegrationTest {

    @Test
    @DisplayName("replaying a request returns 200 with the original transfer and moves no money")
    void replayReturnsOriginal() {
        UUID alice = openAccount("Alice", "100.00");
        UUID bob = openAccount("Bob", "0.00");

        ResponseEntity<Map> first = transfer("key-idem-1", alice, bob, "25.00");
        ResponseEntity<Map> second = transfer("key-idem-1", alice, bob, "25.00");

        assertThat(first.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(second.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(second.getBody().get("transferId")).isEqualTo(first.getBody().get("transferId"));

        assertThat(balanceOf(alice)).isEqualByComparingTo("75.00");
        Long transfers = jdbc.queryForObject("select count(*) from transfers", Long.class);
        assertThat(transfers).isEqualTo(1);
    }

    @Test
    @DisplayName("the same key with a different amount is a 409, not a silent replay")
    void reusingKeyForDifferentBodyConflicts() {
        UUID alice = openAccount("Alice", "100.00");
        UUID bob = openAccount("Bob", "0.00");

        transfer("key-idem-2", alice, bob, "25.00");
        ResponseEntity<Map> mismatch = transfer("key-idem-2", alice, bob, "26.00");

        assertThat(mismatch.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        assertThat(balanceOf(alice)).isEqualByComparingTo("75.00");
    }

    @Test
    @DisplayName("ten simultaneous identical requests produce one transfer and one event")
    void concurrentIdenticalRequestsCollapseToOne() throws Exception {
        UUID alice = openAccount("Alice", "100.00");
        UUID bob = openAccount("Bob", "0.00");

        try (TestEventConsumer events =
                     new TestEventConsumer(SharedContainers.bootstrapServers(), Topics.TRANSFER_COMPLETED);
             ExecutorService pool = Executors.newFixedThreadPool(10)) {

            // All ten miss the idempotency lookup at roughly the same moment, so nine of them hit
            // the unique constraint. That is the path the fast-path lookup cannot cover.
            List<Callable<ResponseEntity<Map>>> calls = java.util.stream.IntStream.range(0, 10)
                    .<Callable<ResponseEntity<Map>>>mapToObj(i ->
                            () -> transfer("key-idem-race", alice, bob, "25.00"))
                    .toList();

            List<Future<ResponseEntity<Map>>> results = pool.invokeAll(calls);

            List<Object> transferIds = results.stream()
                    .map(future -> {
                        try {
                            return future.get();
                        } catch (Exception failure) {
                            throw new AssertionError(failure);
                        }
                    })
                    .peek(response -> assertThat(response.getStatusCode().is2xxSuccessful()).isTrue())
                    .map(response -> response.getBody().get("transferId"))
                    .distinct()
                    .collect(Collectors.toList());

            // Every caller got the same transfer back, whether it created it or replayed it.
            assertThat(transferIds).hasSize(1);
            assertThat(balanceOf(alice)).isEqualByComparingTo("75.00");

            Long transfers = jdbc.queryForObject("select count(*) from transfers", Long.class);
            assertThat(transfers).isEqualTo(1);

            // One transfer, one event. A duplicate here would notify the customer twice.
            List<ConsumerRecord<String, String>> received = events.poll(1, Duration.ofSeconds(30));
            assertThat(received).hasSize(1);
        }
    }
}
