package dev.aleck.wallet.adapter.out.outbox;

import dev.aleck.wallet.application.port.out.OutboxPort;
import java.time.Clock;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import tools.jackson.databind.ObjectMapper;

/**
 * Appends outbox rows with plain SQL.
 *
 * <p>JDBC rather than JPA on purpose. The outbox is append-only and its payload is {@code jsonb},
 * neither of which benefits from an ORM — and the relay has to read it with
 * {@code FOR UPDATE SKIP LOCKED}, which is raw SQL regardless. One access style for the whole
 * table beats half an entity.
 *
 * <p>It still joins the surrounding JPA transaction: Spring Boot hands the DataSource to the
 * {@code JpaTransactionManager}, so a {@code JdbcTemplate} on the same DataSource gets the same
 * connection. That is what makes "transfer and outbox row commit together" true rather than
 * aspirational.
 */
@Repository
class OutboxJdbcAdapter implements OutboxPort {

    private static final String INSERT = """
            insert into outbox_events
                (id, aggregate_type, aggregate_id, event_type, partition_key, payload, created_at)
            values
                (:id, :aggregateType, :aggregateId, :eventType, :partitionKey, cast(:payload as jsonb), :createdAt)
            """;

    private final NamedParameterJdbcTemplate jdbc;
    private final ObjectMapper objectMapper;
    private final Clock clock;

    OutboxJdbcAdapter(NamedParameterJdbcTemplate jdbc, ObjectMapper objectMapper, Clock clock) {
        this.jdbc = jdbc;
        this.objectMapper = objectMapper;
        this.clock = clock;
    }

    @Override
    public void append(OutboxMessage message) {
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("id", message.eventId())
                .addValue("aggregateType", message.aggregateType())
                .addValue("aggregateId", message.aggregateId())
                .addValue("eventType", message.eventType())
                .addValue("partitionKey", message.partitionKey())
                .addValue("payload", objectMapper.writeValueAsString(message.payload()))
                .addValue("createdAt", java.sql.Timestamp.from(clock.instant()));

        jdbc.update(INSERT, params);
    }
}
