package dev.aleck.wallet.support;

import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.springframework.test.context.DynamicPropertyRegistry;

/**
 * One Postgres and one Kafka for the whole test run.
 *
 * <p>Started from a static initialiser and never stopped, rather than with {@code @Container} per
 * class. Testcontainers' Ryuk reaper cleans them up when the JVM exits, and the alternative —
 * restarting a broker between test classes — costs more wall-clock time than every test here
 * takes to run.
 */
public final class SharedContainers {

    /** Pinned to the same versions as docker-compose.yml, so tests exercise what you run locally. */
    private static final PostgreSQLContainer POSTGRES =
            new PostgreSQLContainer("postgres:17-alpine")
                    .withDatabaseName("wallet")
                    .withUsername("wallet")
                    .withPassword("wallet");

    private static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.1.0");

    static {
        POSTGRES.start();
        KAFKA.start();
    }

    private SharedContainers() {
    }

    public static void registerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }

    public static String bootstrapServers() {
        return KAFKA.getBootstrapServers();
    }

    public static String jdbcUrl() {
        return POSTGRES.getJdbcUrl();
    }
}
