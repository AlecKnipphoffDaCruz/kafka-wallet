package dev.aleck.notification;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @param initialInterval wait before the first retry
 * @param multiplier      growth factor per attempt
 * @param maxInterval      ceiling, so a long retry chain cannot stall a partition indefinitely
 * @param maxAttempts     retries <em>after</em> the first delivery, which is how Spring's
 *                        {@code ExponentialBackOff} counts them — 4 here means 5 delivery attempts
 *                        in total, then the dead letter topic
 */
@ConfigurationProperties(prefix = "notification.consumer.retry")
public record ConsumerRetryProperties(
        Duration initialInterval,
        double multiplier,
        Duration maxInterval,
        int maxAttempts) {

    public ConsumerRetryProperties {
        if (initialInterval == null) {
            initialInterval = Duration.ofMillis(500);
        }
        if (multiplier < 1.0) {
            multiplier = 2.0;
        }
        if (maxInterval == null) {
            maxInterval = Duration.ofSeconds(10);
        }
        if (maxAttempts <= 0) {
            maxAttempts = 4;
        }
    }
}
