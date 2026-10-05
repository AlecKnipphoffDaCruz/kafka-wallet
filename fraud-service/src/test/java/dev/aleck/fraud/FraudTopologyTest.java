package dev.aleck.fraud;

import static org.assertj.core.api.Assertions.assertThat;

import dev.aleck.wallet.events.FraudAlert;
import dev.aleck.wallet.events.Topics;
import dev.aleck.wallet.events.TransferCompleted;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

/**
 * Tests the topology with {@link TopologyTestDriver} — no broker, no containers, milliseconds per
 * test.
 *
 * <p>The driver is what makes the windowed rule practical to test. Record time is supplied with
 * each input, so "six transfers in one minute" and "six transfers spread over five minutes" are
 * two deterministic test cases instead of two tests with sleeps in them.
 */
class FraudTopologyTest {

    private static final BigDecimal THRESHOLD = new BigDecimal("10000.0000");
    private static final Duration WINDOW = Duration.ofMinutes(1);
    private static final int MAX_TRANSFERS = 5;

    private static final Instant T0 = Instant.parse("2026-10-05T12:00:00Z");

    private TopologyTestDriver driver;
    private TestInputTopic<String, TransferCompleted> transfers;
    private TestOutputTopic<String, FraudAlert> alerts;

    @BeforeEach
    void setUp() {
        ObjectMapper objectMapper = JsonMapper.builder().build();
        FraudTopology topology = new FraudTopology();

        Serde<TransferCompleted> transferSerde = topology.transferCompletedSerde(objectMapper);
        Serde<FraudAlert> alertSerde = topology.fraudAlertSerde(objectMapper);

        StreamsBuilder builder = new StreamsBuilder();
        topology.fraudPipeline(
                builder,
                transferSerde,
                alertSerde,
                new FraudRuleProperties(THRESHOLD, WINDOW, MAX_TRANSFERS),
                Clock.fixed(T0, ZoneOffset.UTC));

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "fraud-service-test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");

        driver = new TopologyTestDriver(builder.build(), config);
        transfers = driver.createInputTopic(
                Topics.TRANSFER_COMPLETED, Serdes.String().serializer(), transferSerde.serializer());
        alerts = driver.createOutputTopic(
                Topics.FRAUD_ALERT, Serdes.String().deserializer(), alertSerde.deserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    @Test
    @DisplayName("a transfer at or above the threshold raises one alert")
    void amountRuleFires() {
        UUID account = UUID.randomUUID();

        transfers.pipeInput(account.toString(), transfer(account, "10000.0000"), T0);

        List<KeyValue<String, FraudAlert>> produced = alerts.readKeyValuesToList();
        assertThat(produced).hasSize(1);

        FraudAlert alert = produced.getFirst().value;
        assertThat(produced.getFirst().key).isEqualTo(account.toString());
        assertThat(alert.payload().reason()).isEqualTo(FraudAlert.Reason.AMOUNT_ABOVE_THRESHOLD);
        assertThat(alert.payload().accountId()).isEqualTo(account);
        assertThat(alert.payload().amount()).isEqualByComparingTo("10000.0000");
    }

    @Test
    @DisplayName("a transfer below the threshold raises nothing")
    void amountRuleStaysQuiet() {
        UUID account = UUID.randomUUID();

        transfers.pipeInput(account.toString(), transfer(account, "9999.9999"), T0);

        assertThat(alerts.isEmpty()).isTrue();
    }

    @Test
    @DisplayName("the same transfer always produces the same eventId, so replays are deduplicable")
    void alertIdsAreDeterministic() {
        UUID account = UUID.randomUUID();
        TransferCompleted event = transfer(account, "20000.0000");

        transfers.pipeInput(account.toString(), event, T0);
        transfers.pipeInput(account.toString(), event, T0.plusSeconds(1));

        List<KeyValue<String, FraudAlert>> produced = alerts.readKeyValuesToList();
        assertThat(produced).hasSize(2);
        assertThat(produced.get(0).value.eventId()).isEqualTo(produced.get(1).value.eventId());
    }

    @Test
    @DisplayName("exceeding the limit inside one window raises exactly one alert")
    void velocityRuleFiresOncePerWindow() {
        UUID account = UUID.randomUUID();

        // Six transfers, ten seconds apart, all inside the same one-minute window.
        for (int i = 0; i <= MAX_TRANSFERS; i++) {
            transfers.pipeInput(account.toString(), transfer(account, "1.0000"), T0.plusSeconds(i * 10L));
        }

        List<FraudAlert> velocityAlerts = alerts.readKeyValuesToList().stream()
                .map(kv -> kv.value)
                .filter(alert -> alert.payload().reason() == FraudAlert.Reason.VELOCITY_IN_WINDOW)
                .toList();

        // count() emits on every update, so without the "fires on the exact crossing value" filter
        // this would be one alert per transfer past the limit.
        assertThat(velocityAlerts).hasSize(1);
        assertThat(velocityAlerts.getFirst().payload().accountId()).isEqualTo(account);
        assertThat(velocityAlerts.getFirst().payload().transfersInWindow()).isEqualTo(MAX_TRANSFERS + 1);
    }

    @Test
    @DisplayName("the same transfers spread across windows raise nothing: the window is the point")
    void velocityRuleRespectsWindowBoundaries() {
        UUID account = UUID.randomUUID();

        // Six transfers, two minutes apart: never more than one per one-minute window.
        for (int i = 0; i <= MAX_TRANSFERS; i++) {
            transfers.pipeInput(account.toString(), transfer(account, "1.0000"), T0.plus(Duration.ofMinutes(2L * i)));
        }

        assertThat(alerts.isEmpty()).isTrue();
    }

    @Test
    @DisplayName("the limit is per account, not global")
    void velocityRuleIsPerAccount() {
        UUID first = UUID.randomUUID();
        UUID second = UUID.randomUUID();

        // Three each — six transfers in the window, but neither account passes the limit of five.
        for (int i = 0; i < 3; i++) {
            transfers.pipeInput(first.toString(), transfer(first, "1.0000"), T0.plusSeconds(i * 5L));
            transfers.pipeInput(second.toString(), transfer(second, "1.0000"), T0.plusSeconds(i * 5L));
        }

        assertThat(alerts.isEmpty()).isTrue();
    }

    private static TransferCompleted transfer(UUID fromAccount, String amount) {
        return new TransferCompleted(
                UUID.randomUUID(),
                TransferCompleted.TYPE,
                T0,
                new TransferCompleted.Payload(
                        UUID.randomUUID(), fromAccount, UUID.randomUUID(), new BigDecimal(amount), "BRL"));
    }
}
