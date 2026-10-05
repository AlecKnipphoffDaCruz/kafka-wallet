package dev.aleck.wallet.adapter.out.outbox;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

/** The relay's half of the outbox table: claim a batch, mark it done, record failures. */
@Repository
class OutboxStore {

    /**
     * {@code FOR UPDATE SKIP LOCKED} is what lets this relay scale horizontally. Each instance
     * claims rows no one else holds and skips the rest instead of queueing behind them, so two
     * relays never publish the same event and never block each other.
     *
     * <p>{@code order by created_at} keeps publication roughly in the order events happened. Within
     * one partition key, that is what preserves the ordering consumers rely on.
     */
    private static final String CLAIM_BATCH = """
            select id, event_type, partition_key, payload::text as payload_json, created_at, attempts
            from outbox_events
            where published_at is null
            order by created_at, id
            limit :batchSize
            for update skip locked
            """;

    private static final String MARK_PUBLISHED = """
            update outbox_events
            set published_at = :publishedAt, last_error = null
            where id in (:ids)
            """;

    private static final String RECORD_FAILURE = """
            update outbox_events
            set attempts = attempts + 1, last_error = :lastError
            where id in (:ids)
            """;

    private static final String COUNT_BACKLOG = """
            select count(*) from outbox_events where published_at is null
            """;

    private final NamedParameterJdbcTemplate jdbc;

    OutboxStore(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    List<PendingEvent> claimBatch(int batchSize) {
        return jdbc.query(
                CLAIM_BATCH,
                new MapSqlParameterSource("batchSize", batchSize),
                (rs, rowNum) -> new PendingEvent(
                        rs.getObject("id", UUID.class),
                        rs.getString("event_type"),
                        rs.getString("partition_key"),
                        rs.getString("payload_json"),
                        rs.getTimestamp("created_at").toInstant(),
                        rs.getInt("attempts")));
    }

    void markPublished(List<UUID> ids, Instant publishedAt) {
        if (ids.isEmpty()) {
            return;
        }
        jdbc.update(MARK_PUBLISHED, new MapSqlParameterSource()
                .addValue("ids", ids)
                .addValue("publishedAt", java.sql.Timestamp.from(publishedAt)));
    }

    void recordFailure(List<UUID> ids, String lastError) {
        if (ids.isEmpty()) {
            return;
        }
        jdbc.update(RECORD_FAILURE, new MapSqlParameterSource()
                .addValue("ids", ids)
                .addValue("lastError", truncate(lastError)));
    }

    long backlogSize() {
        Long count = jdbc.getJdbcOperations().queryForObject(COUNT_BACKLOG, Long.class);
        return count == null ? 0L : count;
    }

    private static String truncate(String message) {
        if (message == null) {
            return null;
        }
        return message.length() <= 1000 ? message : message.substring(0, 1000);
    }
}
