package dev.aleck.notification.support;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Publishes raw JSON straight to a topic, standing in for wallet-api's relay.
 *
 * <p>Raw strings rather than serialised objects on purpose: these tests need to send a malformed
 * message, which an object-based producer could not express.
 */
public final class TestEventProducer implements AutoCloseable {

    private final KafkaProducer<String, String> producer;

    public TestEventProducer(String bootstrapServers) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        this.producer = new KafkaProducer<>(props);
    }

    /** Sends and waits for the acknowledgement, so a test never races its own input. */
    public void send(String topic, String key, String json) {
        try {
            producer.send(new ProducerRecord<>(topic, key, json)).get();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(interrupted);
        } catch (Exception failure) {
            throw new IllegalStateException("could not publish test event", failure);
        }
    }

    @Override
    public void close() {
        producer.close();
    }
}
