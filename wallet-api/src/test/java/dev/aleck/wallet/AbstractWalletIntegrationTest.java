package dev.aleck.wallet;

import dev.aleck.wallet.support.SharedContainers;
import java.math.BigDecimal;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.resttestclient.TestRestTemplate;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureTestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/** Boots the real application against a real Postgres and a real broker. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
// Boot 4 no longer registers TestRestTemplate implicitly for a running-server test.
@AutoConfigureTestRestTemplate
abstract class AbstractWalletIntegrationTest {

    @Autowired
    protected TestRestTemplate rest;

    @Autowired
    protected JdbcTemplate jdbc;

    @DynamicPropertySource
    static void containerProperties(DynamicPropertyRegistry registry) {
        SharedContainers.registerProperties(registry);
    }

    @BeforeEach
    void cleanDatabase() {
        // Truncated rather than rolled back: the relay publishes on its own scheduler thread, in
        // its own transaction, so a test-managed rollback would not reach it.
        jdbc.execute("truncate table ledger_entries, outbox_events, transfers, accounts cascade");
    }

    protected UUID openAccount(String owner, String balance) {
        ResponseEntity<Map> response = rest.postForEntity(
                "/accounts",
                Map.of("ownerName", owner, "initialBalance", balance, "currency", "BRL"),
                Map.class);

        if (!response.getStatusCode().is2xxSuccessful()) {
            throw new AssertionError("could not open account: " + response.getBody());
        }
        return UUID.fromString((String) response.getBody().get("id"));
    }

    protected ResponseEntity<Map> transfer(String idempotencyKey, UUID from, UUID to, String amount) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set("Idempotency-Key", idempotencyKey);

        return rest.exchange(
                "/transfers",
                HttpMethod.POST,
                new HttpEntity<>(Map.of(
                        "fromAccountId", from.toString(),
                        "toAccountId", to.toString(),
                        "amount", amount,
                        "currency", "BRL"), headers),
                Map.class);
    }

    protected BigDecimal balanceOf(UUID accountId) {
        return jdbc.queryForObject(
                "select balance from accounts where id = ?", BigDecimal.class, accountId);
    }

    protected long unpublishedOutboxRows() {
        return jdbc.queryForObject(
                "select count(*) from outbox_events where published_at is null", Long.class);
    }

    /** Zero for a correct double-entry book, for any sequence of transfers. */
    protected BigDecimal ledgerSignedSum() {
        return jdbc.queryForObject(
                "select coalesce(sum(signed_amount), 0) from ledger_entries", BigDecimal.class);
    }
}
