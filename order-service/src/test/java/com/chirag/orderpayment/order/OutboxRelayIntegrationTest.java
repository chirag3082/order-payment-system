package com.chirag.orderpayment.order;

import com.chirag.orderpayment.common.Topics;
import com.chirag.orderpayment.order.domain.Order;
import com.chirag.orderpayment.order.domain.OutboxEvent;
import com.chirag.orderpayment.order.repo.OutboxRepository;
import com.chirag.orderpayment.order.service.OrderService;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class OutboxRelayIntegrationTest extends IntegrationTestBase {

    @Autowired
    OrderService orderService;

    @Autowired
    OutboxRepository outbox;

    private Consumer<String, String> testConsumer(String group) {
        Map<String, Object> props = KafkaTestUtils.consumerProps(KAFKA.getBootstrapServers(), group, "true");
        Consumer<String, String> consumer = new DefaultKafkaConsumerFactory<>(
                props, new StringDeserializer(), new StringDeserializer()).createConsumer();
        consumer.subscribe(List.of(Topics.ORDER_EVENTS));
        return consumer;
    }

    /**
     * The containers (and so the topic) are shared across tests, and a new group reads
     * from the earliest offset, so other tests' events show up too. Wait for the record
     * with this key instead of expecting it to be the only one.
     */
    private ConsumerRecord<String, String> awaitRecordWithKey(Consumer<String, String> consumer, String key) {
        List<ConsumerRecord<String, String>> seen = new ArrayList<>();
        await().atMost(Duration.ofSeconds(15)).until(() -> {
            consumer.poll(Duration.ofMillis(500)).forEach(seen::add);
            return seen.stream().anyMatch(r -> key.equals(r.key()));
        });
        return seen.stream().filter(r -> key.equals(r.key())).findFirst().orElseThrow();
    }

    @Test
    void placingOrderRelaysOrderPlacedEventToKafka() {
        try (Consumer<String, String> consumer = testConsumer("test-relay")) {
            Order order = orderService.placeOrder("cust-1", new BigDecimal("42.00"), "USD");

            ConsumerRecord<String, String> record = awaitRecordWithKey(consumer, order.getId());

            assertThat(record.value()).contains(order.getId()).contains("cust-1");
            // The outbox row must be marked published once relayed.
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(outbox.findAll()).allMatch(OutboxEvent::isPublished));
        }
    }

    @Test
    void crashLeftUnpublishedRowIsRelayedOncePollerRuns() {
        // Simulate a row committed to the DB before the process died pre-publish.
        String eventId = UUID.randomUUID().toString();
        outbox.save(new OutboxEvent(eventId, "order", "order-xyz", "OrderPlaced",
                Topics.ORDER_EVENTS, "{\"eventId\":\"" + eventId + "\",\"orderId\":\"order-xyz\"}",
                "corr-1"));

        try (Consumer<String, String> consumer = testConsumer("test-recovery")) {
            ConsumerRecord<String, String> record = awaitRecordWithKey(consumer, "order-xyz");
            assertThat(record.value()).contains(eventId);
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(outbox.findById(eventId)).get()
                            .matches(OutboxEvent::isPublished));
        }
    }
}
