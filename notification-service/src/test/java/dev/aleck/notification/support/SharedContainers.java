package dev.aleck.notification.support;

import dev.aleck.wallet.events.Topics;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

/** One Postgres and one Kafka for this module's whole test run. */
public final class SharedContainers {

    private static final PostgreSQLContainer POSTGRES =
            new PostgreSQLContainer("postgres:17-alpine")
                    .withDatabaseName("notifications")
                    .withUsername("wallet")
                    .withPassword("wallet");

    private static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.1.0");

    static {
        POSTGRES.start();
        KAFKA.start();
        createTopics();
    }

    private SharedContainers() {
    }

    /**
     * Creates the topics with the same shape wallet-api declares.
     *
     * <p>Letting the broker auto-create them would give one partition each, which would quietly
     * make the three-thread listener meaningless and hide any partition-related mistake.
     */
    private static void createTopics() {
        try (Admin admin = Admin.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()))) {

            admin.createTopics(List.of(
                            new NewTopic(Topics.TRANSFER_COMPLETED, 3, (short) 1),
                            new NewTopic(Topics.TRANSFER_COMPLETED_DLT, 1, (short) 1)))
                    .all()
                    .get();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(interrupted);
        } catch (ExecutionException failure) {
            throw new IllegalStateException("could not create test topics", failure);
        }
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
}
