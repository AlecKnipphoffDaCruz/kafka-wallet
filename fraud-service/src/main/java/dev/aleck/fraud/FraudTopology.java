package dev.aleck.fraud;

import dev.aleck.wallet.events.FraudAlert;
import dev.aleck.wallet.events.Topics;
import dev.aleck.wallet.events.TransferCompleted;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.UUID;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import tools.jackson.databind.ObjectMapper;

/**
 * Two rules over one input topic.
 *
 * <p><b>Rule 1 — amount.</b> Stateless: a single transfer at or above the threshold is flagged
 * immediately.
 *
 * <p><b>Rule 2 — velocity.</b> Stateful: counts transfers per account in a tumbling window.
 * This is the rule that justifies Kafka Streams. The input is already keyed by source account, so
 * {@code groupByKey} needs no repartition topic — every transfer for an account is already on one
 * partition, which is exactly what the relay's choice of message key bought.
 */
@Configuration
class FraudTopology {

    private static final Logger log = LoggerFactory.getLogger(FraudTopology.class);

    private static final String VELOCITY_STORE = "transfers-per-account-per-window";

    @Bean
    Clock systemClock() {
        return Clock.systemUTC();
    }

    @Bean
    Serde<TransferCompleted> transferCompletedSerde(ObjectMapper objectMapper) {
        return new JsonSerde<>(objectMapper, TransferCompleted.class);
    }

    @Bean
    Serde<FraudAlert> fraudAlertSerde(ObjectMapper objectMapper) {
        return new JsonSerde<>(objectMapper, FraudAlert.class);
    }

    @Bean
    KStream<String, TransferCompleted> fraudPipeline(
            StreamsBuilder builder,
            Serde<TransferCompleted> transferSerde,
            Serde<FraudAlert> alertSerde,
            FraudRuleProperties rules,
            Clock clock) {

        KStream<String, TransferCompleted> transfers = builder
                .stream(Topics.TRANSFER_COMPLETED, Consumed.with(Serdes.String(), transferSerde))
                // A null value is a tombstone and a null key cannot be grouped; neither is a
                // transfer. The deserialisation handler already drops unparseable records.
                .filter((accountId, event) -> accountId != null && event != null && event.payload() != null);

        amountRule(transfers, alertSerde, rules, clock);
        velocityRule(transfers, transferSerde, alertSerde, rules, clock);

        return transfers;
    }

    private void amountRule(
            KStream<String, TransferCompleted> transfers,
            Serde<FraudAlert> alertSerde,
            FraudRuleProperties rules,
            Clock clock) {

        transfers
                .filter((accountId, event) ->
                        event.payload().amount().compareTo(rules.amountThreshold()) >= 0)
                .peek((accountId, event) -> log.info("amount rule tripped by transfer {} ({})",
                        event.payload().transferId(), event.payload().amount()))
                .mapValues((accountId, event) -> new FraudAlert(
                        // Derived, not random: reprocessing the topic produces byte-identical
                        // alerts, so downstream consumers can dedupe on eventId even though
                        // Streams may replay after a rebalance or a state restore.
                        deterministicId("amount", event.payload().transferId().toString()),
                        FraudAlert.TYPE,
                        clock.instant(),
                        new FraudAlert.Payload(
                                event.payload().fromAccount(),
                                FraudAlert.Reason.AMOUNT_ABOVE_THRESHOLD,
                                "transfer of %s %s is at or above the %s threshold".formatted(
                                        event.payload().amount().toPlainString(),
                                        event.payload().currency(),
                                        rules.amountThreshold().toPlainString()),
                                event.payload().transferId(),
                                event.payload().amount(),
                                1)))
                .to(Topics.FRAUD_ALERT, Produced.with(Serdes.String(), alertSerde));
    }

    private void velocityRule(
            KStream<String, TransferCompleted> transfers,
            Serde<TransferCompleted> transferSerde,
            Serde<FraudAlert> alertSerde,
            FraudRuleProperties rules,
            Clock clock) {

        transfers
                .groupByKey(Grouped.with(Serdes.String(), transferSerde))
                // Tumbling, with no grace period: this is a near-real-time alert, so a late event
                // arriving after its window closed is better dropped than acted on minutes late.
                // A reconciliation job would make the opposite choice.
                .windowedBy(TimeWindows.ofSizeWithNoGrace(rules.velocityWindow()))
                .count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>as(VELOCITY_STORE))
                .toStream()
                // count() emits on every update, so an account at 9 transfers in a window would
                // produce an alert at 6, 7, 8 and 9. Firing only on the exact crossing value gives
                // one alert per account per window.
                .filter((window, count) -> count != null && count == rules.velocityMaxTransfers() + 1L)
                .map((window, count) -> KeyValue.pair(
                        window.key(),
                        velocityAlert(window, count, rules, clock)))
                .peek((accountId, alert) -> log.info("velocity rule tripped by account {}", accountId))
                .to(Topics.FRAUD_ALERT, Produced.with(Serdes.String(), alertSerde));
    }

    private static FraudAlert velocityAlert(
            Windowed<String> window, Long count, FraudRuleProperties rules, Clock clock) {

        return new FraudAlert(
                // The window start is part of the identity: the same account tripping the rule in
                // two different minutes is two alerts, not a duplicate of one.
                deterministicId("velocity", window.key() + "|" + window.window().start()),
                FraudAlert.TYPE,
                clock.instant(),
                new FraudAlert.Payload(
                        UUID.fromString(window.key()),
                        FraudAlert.Reason.VELOCITY_IN_WINDOW,
                        "%d transfers in %s, limit is %d".formatted(
                                count, rules.velocityWindow(), rules.velocityMaxTransfers()),
                        null,
                        null,
                        count.intValue()));
    }

    /**
     * A name-based (v3) UUID over the rule and what it fired on, so the same input always yields
     * the same {@code eventId}.
     */
    private static UUID deterministicId(String rule, String subject) {
        return UUID.nameUUIDFromBytes((rule + "|" + subject).getBytes(StandardCharsets.UTF_8));
    }
}
