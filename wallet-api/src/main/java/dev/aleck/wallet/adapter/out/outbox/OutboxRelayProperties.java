package dev.aleck.wallet.adapter.out.outbox;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @param batchSize    how many events one tick claims; bounds both the lock duration and the size
 *                     of a batch that has to be retried after a failure
 * @param sendTimeout  how long to wait for Kafka acknowledgements before giving up the batch
 */
@ConfigurationProperties(prefix = "wallet.outbox")
public record OutboxRelayProperties(
        int batchSize,
        Duration sendTimeout) {

    public OutboxRelayProperties {
        if (batchSize <= 0) {
            batchSize = 100;
        }
        if (sendTimeout == null) {
            sendTimeout = Duration.ofSeconds(10);
        }
    }
}
