package com.chirag.orderpayment.payment.messaging;

import com.chirag.orderpayment.common.event.OrderPlacedEvent;
import com.chirag.orderpayment.common.event.PaymentResultEvent;
import com.chirag.orderpayment.payment.service.PaymentProcessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumes OrderPlaced events, charges payment idempotently, and publishes the
 * result. Any exception thrown here is handled by the container's error handler,
 * which retries with backoff and finally routes poison messages to the DLT.
 */
@Component
public class OrderEventListener {

    private static final Logger log = LoggerFactory.getLogger(OrderEventListener.class);

    private final PaymentProcessor processor;
    private final PaymentResultPublisher publisher;
    private final ObjectMapper objectMapper;

    public OrderEventListener(PaymentProcessor processor, PaymentResultPublisher publisher,
                              ObjectMapper objectMapper) {
        this.processor = processor;
        this.publisher = publisher;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "#{T(com.chirag.orderpayment.common.Topics).ORDER_EVENTS}",
            groupId = "payment-service")
    public void onOrderPlaced(String payload) throws Exception {
        OrderPlacedEvent order = objectMapper.readValue(payload, OrderPlacedEvent.class);
        log.debug("Received OrderPlaced for order {}", order.orderId());
        PaymentResultEvent result = processor.process(order);
        publisher.publish(result);
    }
}
