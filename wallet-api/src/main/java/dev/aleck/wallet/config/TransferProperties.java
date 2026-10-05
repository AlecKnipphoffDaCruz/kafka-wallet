package dev.aleck.wallet.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @param maxAttempts       how many times a transfer may lose the optimistic-lock race before the
 *                          client gets a 409. Too low and ordinary contention becomes a visible
 *                          error; too high and a genuinely hot account ties up request threads.
 * @param backoffBaseMillis first backoff ceiling; doubles per attempt, with full jitter
 */
@ConfigurationProperties(prefix = "wallet.transfer")
public record TransferProperties(int maxAttempts, long backoffBaseMillis) {

    public TransferProperties {
        if (maxAttempts <= 0) {
            maxAttempts = 5;
        }
        if (backoffBaseMillis <= 0) {
            backoffBaseMillis = 20;
        }
    }
}
