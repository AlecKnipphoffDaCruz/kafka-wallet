package dev.aleck.wallet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import dev.aleck.wallet.events.Topics;
import dev.aleck.wallet.support.SharedContainers;
import dev.aleck.wallet.support.TestEventConsumer;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.IntStream;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.TestPropertySource;

/**
 * Fifty concurrent transfers out of one account.
 *
 * <p>The question this answers is whether optimistic locking actually prevents a lost update. With
 * it removed, this test fails loudly and non-deterministically: every thread reads the same balance
 * and the last write wins, so the account ends up having paid out far less than it sent.
 */
@TestPropertySource(properties = {
        // Fifty threads contending on two rows is far past what the production default is tuned
        // for. Raising the budget here keeps the test about correctness rather than about how many
        // 409s a thundering herd produces.
        "wallet.transfer.max-attempts=60",
        "wallet.transfer.backoff-base-millis=5"
})
class ConcurrentTransfersIT extends AbstractWalletIntegrationTest {

    private static final int TRANSFERS = 50;

    @Test
    @DisplayName("the final balance equals the starting balance minus every transfer that succeeded")
    void balancesSurviveContention() throws Exception {
        UUID alice = openAccount("Alice", "100.00");
        UUID bob = openAccount("Bob", "0.00");

        try (TestEventConsumer events =
                     new TestEventConsumer(SharedContainers.bootstrapServers(), Topics.TRANSFER_COMPLETED);
             ExecutorService pool = Executors.newFixedThreadPool(16)) {

            // Released together, so the requests genuinely overlap instead of queueing behind the
            // executor's ramp-up.
            CountDownLatch startLine = new CountDownLatch(1);

            List<Callable<ResponseEntity<Map>>> calls = IntStream.range(0, TRANSFERS)
                    .<Callable<ResponseEntity<Map>>>mapToObj(i -> () -> {
                        startLine.await();
                        return transfer("key-concurrent-" + i, alice, bob, "1.00");
                    })
                    .toList();

            List<Future<ResponseEntity<Map>>> futures = calls.stream().map(pool::submit).toList();
            startLine.countDown();

            int created = 0;
            int conflicted = 0;
            for (Future<ResponseEntity<Map>> future : futures) {
                HttpStatus status = (HttpStatus) future.get().getStatusCode();
                if (status == HttpStatus.CREATED) {
                    created++;
                } else if (status == HttpStatus.CONFLICT) {
                    conflicted++;
                } else {
                    throw new AssertionError("unexpected status " + status);
                }
            }

            assertThat(created + conflicted).isEqualTo(TRANSFERS);
            assertThat(created).as("at least most transfers should get through").isGreaterThan(0);

            // The invariant, stated against what actually succeeded rather than what we hoped
            // would: no lost updates, no money created, no money destroyed.
            BigDecimal moved = new BigDecimal(created).setScale(2);
            assertThat(balanceOf(alice)).isEqualByComparingTo(new BigDecimal("100.00").subtract(moved));
            assertThat(balanceOf(bob)).isEqualByComparingTo(moved);
            assertThat(ledgerSignedSum()).isEqualByComparingTo(BigDecimal.ZERO);

            Long transfers = jdbc.queryForObject("select count(*) from transfers", Long.class);
            assertThat(transfers).isEqualTo(created);
            Long entries = jdbc.queryForObject("select count(*) from ledger_entries", Long.class);
            assertThat(entries).isEqualTo(created * 2L);

            // Exactly one event per committed transfer: the outbox drains completely and invents
            // nothing. The relay is at-least-once, so poll for a little more than expected to
            // catch an accidental duplicate instead of ignoring it.
            await().atMost(Duration.ofSeconds(30))
                    .untilAsserted(() -> assertThat(unpublishedOutboxRows()).isZero());

            List<ConsumerRecord<String, String>> received =
                    events.poll(created, Duration.ofSeconds(30));
            assertThat(received).hasSize(created);

            // All from one account, so all on one partition — which is what makes their order
            // meaningful to a consumer.
            assertThat(received).extracting(ConsumerRecord::partition).containsOnly(received.getFirst().partition());
        }
    }
}
