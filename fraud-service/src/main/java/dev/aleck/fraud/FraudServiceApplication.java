package dev.aleck.fraud;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.kafka.annotation.EnableKafkaStreams;

/**
 * Scores transfers for fraud with Kafka Streams.
 *
 * <p>Streams rather than a plain consumer for one reason: the velocity rule is stateful. "More than
 * N transfers from one account in a minute" needs a per-account counter that survives restarts and
 * follows a partition when it is reassigned. Hand-rolling that means a database, a scheduler and a
 * story for what happens during a rebalance; Streams gives it as a windowed store backed by a
 * changelog topic.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
@EnableKafkaStreams
public class FraudServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(FraudServiceApplication.class, args);
    }
}
