package dev.aleck.fraud;

import java.math.BigDecimal;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @param amountThreshold      a single transfer at or above this amount is flagged
 * @param velocityWindow       length of the tumbling window the velocity rule counts over
 * @param velocityMaxTransfers transfers allowed per account per window before an alert fires
 */
@ConfigurationProperties(prefix = "fraud.rules")
public record FraudRuleProperties(
        BigDecimal amountThreshold,
        Duration velocityWindow,
        int velocityMaxTransfers) {

    public FraudRuleProperties {
        if (amountThreshold == null) {
            amountThreshold = new BigDecimal("10000.0000");
        }
        if (velocityWindow == null) {
            velocityWindow = Duration.ofMinutes(1);
        }
        if (velocityMaxTransfers <= 0) {
            velocityMaxTransfers = 5;
        }
    }
}
