package dev.aleck.notification;

import dev.aleck.wallet.events.Topics;
import java.time.Clock;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

/**
 * Retry, backoff and dead-lettering.
 *
 * <p>Spring Boot picks up a {@code CommonErrorHandler} bean and installs it on every listener
 * container, so this one declaration covers the whole service.
 */
@Configuration
class KafkaConsumerConfiguration {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerConfiguration.class);

    @Bean
    Clock systemClock() {
        return Clock.systemUTC();
    }

    /**
     * Blocking retries with exponential backoff, then the dead letter topic.
     *
     * <p>Blocking is the right default here and the trade-off is worth stating: while a record is
     * being retried, its partition makes no progress. That is a feature for a wallet — events for
     * one account must not overtake each other — and the reason the backoff has a hard ceiling and
     * a hard attempt limit instead of retrying forever.
     *
     * <p>{@code @RetryableTopic} is the non-blocking alternative: failures are republished to
     * {@code -retry} topics so the main partition keeps moving. It buys throughput and costs
     * ordering, which is the wrong trade for money movement.
     */
    @Bean
    DefaultErrorHandler kafkaErrorHandler(
            KafkaTemplate<String, String> kafkaTemplate, ConsumerRetryProperties retry) {

        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                // The default resolver keeps the original partition number, which breaks against a
                // single-partition DLQ: a record from partition 2 would be sent to a partition that
                // does not exist. Pinning it to 0 keeps the DLQ readable in arrival order.
                (record, exception) -> new TopicPartition(Topics.TRANSFER_COMPLETED_DLT, 0));

        ExponentialBackOff backOff = new ExponentialBackOff();
        backOff.setInitialInterval(retry.initialInterval().toMillis());
        backOff.setMultiplier(retry.multiplier());
        backOff.setMaxInterval(retry.maxInterval().toMillis());
        backOff.setMaxAttempts(retry.maxAttempts());

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);

        // Messages that cannot ever succeed skip the retries entirely and go straight to the DLQ.
        errorHandler.addNotRetryableExceptions(NonRetryableEventException.class);

        errorHandler.setRetryListeners((record, exception, deliveryAttempt) ->
                log.warn("delivery attempt {} failed for {}-{}@{}: {}",
                        deliveryAttempt, record.topic(), record.partition(), record.offset(),
                        exception.getMessage()));

        return errorHandler;
    }
}
