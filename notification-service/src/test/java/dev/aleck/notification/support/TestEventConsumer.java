package dev.aleck.notification.support;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * Reads whatever is produced after it is constructed, with a throwaway group id.
 *
 * <p>A fresh group per instance is half of what makes each test independent — a shared group would
 * resume from the previous test's committed offset and see nothing. The other half is seeking to
 * the end on construction: truncating the database between tests does not truncate the topic, so a
 * consumer starting from offset 0 would also count events from every earlier test. (Which is itself
 * the lesson: a Kafka topic is a log, not a queue. Nothing is removed by being read.)
 */
public final class TestEventConsumer implements AutoCloseable {

    private final KafkaConsumer<String, String> consumer;

    public TestEventConsumer(String bootstrapServers, String topic) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        this.consumer = new KafkaConsumer<>(props);
        this.consumer.subscribe(List.of(topic));
        skipExistingHistory();
    }

    /**
     * Waits for partitions to be assigned, then positions at the end of each.
     *
     * <p>The poll loop is not optional: {@code subscribe()} is lazy, so {@code assignment()} is
     * empty until a poll has driven the group join. Seeking before then would silently do nothing.
     */
    private void skipExistingHistory() {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (consumer.assignment().isEmpty() && System.nanoTime() < deadline) {
            consumer.poll(Duration.ofMillis(200));
        }
        if (consumer.assignment().isEmpty()) {
            throw new IllegalStateException("no partitions assigned within 30s");
        }
        consumer.seekToEnd(consumer.assignment());
        // seekToEnd is lazy too; asking for the position forces it to resolve now, before the
        // test produces anything.
        consumer.assignment().forEach(consumer::position);
    }

    /**
     * Polls until {@code expected} records have arrived or the deadline passes.
     *
     * @return everything received, which may be fewer than expected — assert on the result rather
     *         than trusting a single poll, since the first poll of a new group usually returns
     *         nothing while the assignment settles
     */
    public List<ConsumerRecord<String, String>> poll(int expected, Duration timeout) {
        List<ConsumerRecord<String, String>> received = new ArrayList<>();
        long deadline = System.nanoTime() + timeout.toNanos();

        while (received.size() < expected && System.nanoTime() < deadline) {
            ConsumerRecords<String, String> batch = consumer.poll(Duration.ofMillis(250));
            batch.forEach(received::add);
        }
        return received;
    }

    @Override
    public void close() {
        consumer.close();
    }
}
