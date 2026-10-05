package dev.aleck.notification;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/**
 * Consumes {@code transfer.completed} and "notifies" the account holder.
 *
 * <p>Unlike wallet-api this service is not built as a hexagon. It has no domain to protect — it
 * reads an event, writes two rows and is done. The structure a wallet needs would be ceremony
 * here, and picking the right amount of structure per service is itself a design decision.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class NotificationServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(NotificationServiceApplication.class, args);
    }
}
