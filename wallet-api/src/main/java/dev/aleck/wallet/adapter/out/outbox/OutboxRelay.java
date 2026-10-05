package dev.aleck.wallet.adapter.out.outbox;

import dev.aleck.wallet.application.port.out.TransactionRunner;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

/**
 * Moves committed outbox rows onto Kafka.
 *
 * <h2>Why a poller at all</h2>
 * Writing to the database and then producing to Kafka is two commits with no coordination: crash
 * in between and you have either money that moved with no event, or an event for money that never
 * moved. The outbox collapses the first commit into the business transaction; this relay owns the
 * second, and it can fail as often as it likes because the row is still there.
 *
 * <h2>What this guarantees</h2>
 * <b>At-least-once, in key order.</b> If the broker acknowledges a send but this process dies
 * before {@code markPublished} commits, the next tick republishes those events. That is not a bug
 * to be fixed here — removing it would need a distributed transaction. It is a cost paid once, by
 * making consumers idempotent (see {@code notification-service}).
 *
 * <h2>Failure handling</h2>
 * A send failure rolls the whole batch back: rows stay unpublished and are retried next tick, with
 * {@code attempts} and {@code last_error} recorded in a separate transaction so the diagnosis
 * survives the rollback.
 */
@Component
class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    private final OutboxStore store;
    private final TransactionRunner transactionRunner;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final OutboxRelayProperties properties;
    private final Clock clock;

    private final Counter publishedCounter;
    private final Counter failedCounter;
    private final Timer batchTimer;

    OutboxRelay(
            OutboxStore store,
            TransactionRunner transactionRunner,
            KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper,
            OutboxRelayProperties properties,
            Clock clock,
            MeterRegistry meterRegistry) {
        this.store = store;
        this.transactionRunner = transactionRunner;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.properties = properties;
        this.clock = clock;

        this.publishedCounter = Counter.builder("wallet.outbox.published")
                .description("Outbox events acknowledged by the broker")
                .register(meterRegistry);
        this.failedCounter = Counter.builder("wallet.outbox.failed")
                .description("Outbox batches that rolled back and will be retried")
                .register(meterRegistry);
        this.batchTimer = Timer.builder("wallet.outbox.batch")
                .description("Time to claim, publish and mark one batch")
                .register(meterRegistry);

        // The number to watch in Grafana: a backlog that climbs instead of oscillating means the
        // relay is losing ground to the write rate, or Kafka is refusing writes.
        meterRegistry.gauge("wallet.outbox.backlog", store, OutboxStore::backlogSize);
    }

    @Scheduled(
            fixedDelayString = "${wallet.outbox.poll-interval:500}",
            initialDelayString = "${wallet.outbox.initial-delay:1000}")
    void publishPending() {
        try {
            batchTimer.recordCallable(this::publishOneBatch);
        } catch (Exception failure) {
            // Never let the scheduler's thread die on an exception: a dead relay is a silent outage.
            log.error("outbox relay tick failed", failure);
        }
    }

    private int publishOneBatch() {
        List<UUID> claimed = new ArrayList<>();
        try {
            return transactionRunner.inTransaction(() -> {
                List<PendingEvent> batch = store.claimBatch(properties.batchSize());
                if (batch.isEmpty()) {
                    return 0;
                }
                batch.forEach(event -> claimed.add(event.id()));

                List<CompletableFuture<SendResult<String, String>>> inFlight = batch.stream()
                        .map(this::send)
                        .toList();

                // Send the whole batch, then wait once. Waiting per message would serialise the
                // round trips and cap throughput at one event per network latency.
                awaitAll(inFlight);

                store.markPublished(claimed, clock.instant());
                publishedCounter.increment(batch.size());
                log.debug("published {} outbox events", batch.size());
                return batch.size();
            });
        } catch (RuntimeException failure) {
            failedCounter.increment();
            log.warn("outbox batch of {} events failed and will be retried", claimed.size(), failure);
            recordFailureOutsideRolledBackTransaction(claimed, failure);
            return 0;
        }
    }

    private CompletableFuture<SendResult<String, String>> send(PendingEvent event) {
        ProducerRecord<String, String> record = new ProducerRecord<>(
                EventTopicRouter.topicFor(event.eventType()),
                // Null partition: let the default partitioner hash the key. Hard-coding a
                // partition would break the moment the topic is grown.
                null,
                event.partitionKey(),
                EventEnvelopes.render(objectMapper, event));

        // Headers travel with the message so a consumer can dedupe or trace without parsing the
        // body — and so the DLQ stays readable when the body is what went wrong.
        record.headers().add("event-id", event.id().toString().getBytes());
        record.headers().add("event-type", event.eventType().getBytes());

        return kafkaTemplate.send(record);
    }

    private void awaitAll(List<CompletableFuture<SendResult<String, String>>> inFlight) {
        try {
            CompletableFuture.allOf(inFlight.toArray(CompletableFuture[]::new))
                    .get(properties.sendTimeout().toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while publishing outbox batch", interrupted);
        } catch (Exception failure) {
            throw new IllegalStateException("outbox batch was not acknowledged by Kafka", failure);
        }
    }

    /**
     * The batch transaction rolled back, taking any {@code attempts} increment with it. A fresh
     * transaction is the only way to keep a record of why.
     */
    private void recordFailureOutsideRolledBackTransaction(List<UUID> ids, RuntimeException failure) {
        if (ids.isEmpty()) {
            return;
        }
        try {
            transactionRunner.inTransaction(() -> {
                store.recordFailure(ids, failure.toString());
                return null;
            });
        } catch (RuntimeException bookkeepingFailed) {
            log.error("could not record outbox failure for {} events", ids.size(), bookkeepingFailed);
        }
    }
}
