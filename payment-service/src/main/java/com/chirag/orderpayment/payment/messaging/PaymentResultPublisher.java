package com.chirag.orderpayment.payment.messaging;

import com.chirag.orderpayment.common.Headers;
import com.chirag.orderpayment.common.Topics;
import com.chirag.orderpayment.common.event.PaymentResultEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.MDC;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

@Component
public class PaymentResultPublisher {

    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper objectMapper;

    public PaymentResultPublisher(KafkaTemplate<String, String> kafka, ObjectMapper objectMapper) {
        this.kafka = kafka;
        this.objectMapper = objectMapper;
    }

    public void publish(PaymentResultEvent event) {
        ProducerRecord<String, String> record = new ProducerRecord<>(
                Topics.PAYMENT_RESULTS, event.orderId(), serialize(event));
        record.headers().add(new RecordHeader(Headers.EVENT_ID,
                event.eventId().getBytes(StandardCharsets.UTF_8)));
        String correlationId = MDC.get(Headers.CORRELATION_ID_MDC);
        if (correlationId != null) {
            record.headers().add(new RecordHeader(Headers.CORRELATION_ID,
                    correlationId.getBytes(StandardCharsets.UTF_8)));
        }
        kafka.send(record);
    }

    private String serialize(PaymentResultEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize PaymentResultEvent", e);
        }
    }
}
