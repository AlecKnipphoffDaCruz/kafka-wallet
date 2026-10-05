package dev.aleck.notification;

import java.time.Clock;
import java.util.List;
import java.util.UUID;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
class NotificationRepository {

    private static final String INSERT = """
            insert into notifications (id, event_id, transfer_id, account_id, channel, message, created_at)
            values (:id, :eventId, :transferId, :accountId, :channel, :message, :createdAt)
            """;

    private static final String FIND_BY_TRANSFER = """
            select id, event_id, transfer_id, account_id, channel, message, created_at
            from notifications
            where transfer_id = :transferId
            order by created_at
            """;

    private static final String COUNT = "select count(*) from notifications";

    private final NamedParameterJdbcTemplate jdbc;
    private final Clock clock;

    NotificationRepository(NamedParameterJdbcTemplate jdbc, Clock clock) {
        this.jdbc = jdbc;
        this.clock = clock;
    }

    void save(Notification notification) {
        jdbc.update(INSERT, new MapSqlParameterSource()
                .addValue("id", notification.id())
                .addValue("eventId", notification.eventId())
                .addValue("transferId", notification.transferId())
                .addValue("accountId", notification.accountId())
                .addValue("channel", notification.channel())
                .addValue("message", notification.message())
                .addValue("createdAt", java.sql.Timestamp.from(clock.instant())));
    }

    List<Notification> findByTransfer(UUID transferId) {
        return jdbc.query(FIND_BY_TRANSFER,
                new MapSqlParameterSource("transferId", transferId),
                (rs, rowNum) -> new Notification(
                        rs.getObject("id", UUID.class),
                        rs.getObject("event_id", UUID.class),
                        rs.getObject("transfer_id", UUID.class),
                        rs.getObject("account_id", UUID.class),
                        rs.getString("channel"),
                        rs.getString("message")));
    }

    long count() {
        Long count = jdbc.getJdbcOperations().queryForObject(COUNT, Long.class);
        return count == null ? 0L : count;
    }
}
