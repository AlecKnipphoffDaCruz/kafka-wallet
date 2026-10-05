package dev.aleck.fraud;

import java.nio.charset.StandardCharsets;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import tools.jackson.databind.ObjectMapper;

/**
 * A minimal JSON {@link Serde} over the application's own {@code ObjectMapper}.
 *
 * <p>Hand-written rather than Spring's {@code JacksonJsonSerde} on purpose: the event contract is
 * plain JSON with no Java type headers, so anything in any language can produce and consume it.
 * Serdes that embed class names are convenient inside one codebase and a migration problem across
 * services.
 *
 * <p>A {@code null} value round-trips as {@code null} rather than as the four bytes "null", which
 * is what Kafka Streams expects for a tombstone.
 */
final class JsonSerde<T> implements Serde<T> {

    private final ObjectMapper objectMapper;
    private final Class<T> type;

    JsonSerde(ObjectMapper objectMapper, Class<T> type) {
        this.objectMapper = objectMapper;
        this.type = type;
    }

    @Override
    public Serializer<T> serializer() {
        return (topic, data) -> data == null
                ? null
                : objectMapper.writeValueAsString(data).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public Deserializer<T> deserializer() {
        return (topic, bytes) -> bytes == null
                ? null
                : objectMapper.readValue(new String(bytes, StandardCharsets.UTF_8), type);
    }
}
