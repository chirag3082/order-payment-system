package com.chirag.orderpayment.payment;

import com.chirag.orderpayment.common.Topics;
import com.chirag.orderpayment.common.event.OrderPlacedEvent;
import com.chirag.orderpayment.payment.repo.PaymentRepository;
import com.chirag.orderpayment.payment.repo.ProcessedEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class PaymentFlowIntegrationTest extends IntegrationTestBase {

    @Autowired
    KafkaTemplate<String, String> kafka;

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    PaymentRepository payments;

    @Autowired
    ProcessedEventRepository processed;

    private Consumer<String, String> consumerFor(String group, String topic) {
        Map<String, Object> props = KafkaTestUtils.consumerProps(KAFKA.getBootstrapServers(), group, "true");
        Consumer<String, String> consumer = new DefaultKafkaConsumerFactory<>(
                props, new StringDeserializer(), new StringDeserializer()).createConsumer();
        consumer.subscribe(List.of(topic));
        return consumer;
    }

    private void publishOrder(OrderPlacedEvent event) throws Exception {
        kafka.send(Topics.ORDER_EVENTS, event.orderId(), objectMapper.writeValueAsString(event));
    }

    private OrderPlacedEvent order(BigDecimal amount) {
        return new OrderPlacedEvent(UUID.randomUUID().toString(), UUID.randomUUID().toString(),
                "cust-1", amount, "USD", Instant.now());
    }

    @Test
    void approvedOrderProducesApprovedPaymentResult() throws Exception {
        try (Consumer<String, String> results = consumerFor("test-results", Topics.PAYMENT_RESULTS)) {
            OrderPlacedEvent event = order(new BigDecimal("99.00"));
            publishOrder(event);

            ConsumerRecord<String, String> record =
                    KafkaTestUtils.getSingleRecord(results, Topics.PAYMENT_RESULTS, Duration.ofSeconds(20));
            assertThat(record.value()).contains(event.orderId()).contains("APPROVED");
        }
    }

    @Test
    void duplicateOrderEventChargesOnlyOnce() throws Exception {
        OrderPlacedEvent event = order(new BigDecimal("55.00"));
        publishOrder(event);
        publishOrder(event); // duplicate delivery, same eventId

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() ->
                assertThat(processed.existsById(event.eventId())).isTrue());
        // Give any duplicate a chance to (wrongly) create a second row, then assert exactly one.
        Thread.sleep(1000);
        assertThat(payments.findAll().stream()
                .filter(p -> p.getOrderId().equals(event.orderId())).count()).isEqualTo(1);
    }

    @Test
    void poisonMessageIsRoutedToDeadLetterTopic() throws Exception {
        try (Consumer<String, String> dlt = consumerFor("test-dlt", Topics.ORDER_EVENTS_DLT)) {
            OrderPlacedEvent event = order(new BigDecimal("6.66")); // triggers simulated failure
            publishOrder(event);

            ConsumerRecord<String, String> record =
                    KafkaTestUtils.getSingleRecord(dlt, Topics.ORDER_EVENTS_DLT, Duration.ofSeconds(30));
            assertThat(record.value()).contains(event.orderId());
        }
    }
}
