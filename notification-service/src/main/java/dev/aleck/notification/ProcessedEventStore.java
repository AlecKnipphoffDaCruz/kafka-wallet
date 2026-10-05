package dev.aleck.notification;

import java.time.Clock;
import java.util.UUID;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

/** The consumer's memory of what it has already done. */
@Repository
class ProcessedEventStore {

    /**
     * {@code on conflict do nothing} turns "have I seen this?" into a single atomic statement.
     *
     * <p>A {@code select} followed by an {@code insert} would look equivalent and be wrong: two
     * consumer threads handling the same redelivered event can both see "not processed" before
     * either inserts. The primary key is the real guard; the return value just tells us which
     * side of it we landed on.
     */
    private static final String CLAIM = """
            insert into processed_events (event_id, consumer, processed_at)
            values (:eventId, :consumer, :processedAt)
            on conflict (event_id, consumer) do nothing
            """;

    private final NamedParameterJdbcTemplate jdbc;
    private final Clock clock;

    ProcessedEventStore(NamedParameterJdbcTemplate jdbc, Clock clock) {
        this.jdbc = jdbc;
        this.clock = clock;
    }

    /**
     * @return {@code true} if this call claimed the event (it is ours to process), {@code false}
     *         if it had already been processed and should be skipped
     */
    boolean claim(UUID eventId, String consumer) {
        int inserted = jdbc.update(CLAIM, new MapSqlParameterSource()
                .addValue("eventId", eventId)
                .addValue("consumer", consumer)
                .addValue("processedAt", java.sql.Timestamp.from(clock.instant())));

        return inserted == 1;
    }
}
