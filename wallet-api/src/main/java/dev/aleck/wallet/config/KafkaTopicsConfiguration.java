package dev.aleck.wallet.config;

import dev.aleck.wallet.events.Topics;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Declares topics so a fresh environment comes up complete.
 *
 * <p>Relying on {@code auto.create.topics.enable} would create them with the broker's defaults —
 * one partition — and partition count cannot be lowered later. Declaring them is also the only
 * honest place to write down <em>why</em> each has the partition count it has.
 */
@Configuration
class KafkaTopicsConfiguration {

    /**
     * Three partitions: enough to demonstrate parallel consumption and rebalancing, and a
     * reminder that the number is a ceiling on consumers in one group — a fourth instance would
     * sit idle.
     *
     * <p>Growing this later is allowed but not free: the key→partition mapping changes, so events
     * for an account can land on a different partition than its history. Ordering is preserved
     * going forward, not retroactively.
     */
    @Bean
    NewTopic transferCompletedTopic() {
        return TopicBuilder.name(Topics.TRANSFER_COMPLETED)
                .partitions(3)
                .replicas(1)
                .build();
    }

    /**
     * One partition. The DLQ is for humans: nothing consumes it automatically, and a single
     * partition keeps the failures in arrival order while you read them.
     */
    @Bean
    NewTopic transferCompletedDeadLetterTopic() {
        return TopicBuilder.name(Topics.TRANSFER_COMPLETED_DLT)
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    NewTopic fraudAlertTopic() {
        return TopicBuilder.name(Topics.FRAUD_ALERT)
                .partitions(3)
                .replicas(1)
                .build();
    }
}
