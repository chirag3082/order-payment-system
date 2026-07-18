package com.chirag.orderpayment.order.messaging;

import com.chirag.orderpayment.common.event.PaymentResultEvent;
import com.chirag.orderpayment.order.domain.Order;
import com.chirag.orderpayment.order.domain.ProcessedResult;
import com.chirag.orderpayment.order.repo.OrderRepository;
import com.chirag.orderpayment.order.repo.ProcessedResultRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Completes the choreography saga: consumes payment results and confirms or
 * cancels the corresponding order. Idempotent — a duplicate delivery of the same
 * event id is recorded once and skipped, so at-least-once delivery is safe.
 */
@Component
public class PaymentResultListener {

    private static final Logger log = LoggerFactory.getLogger(PaymentResultListener.class);

    private final OrderRepository orders;
    private final ProcessedResultRepository processed;
    private final ObjectMapper objectMapper;

    public PaymentResultListener(OrderRepository orders, ProcessedResultRepository processed,
                                 ObjectMapper objectMapper) {
        this.orders = orders;
        this.processed = processed;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "#{T(com.chirag.orderpayment.common.Topics).PAYMENT_RESULTS}",
            groupId = "order-service")
    @Transactional
    public void onPaymentResult(String payload) throws Exception {
        PaymentResultEvent event = objectMapper.readValue(payload, PaymentResultEvent.class);

        if (processed.existsById(event.eventId())) {
            log.debug("Skipping already-processed payment result {}", event.eventId());
            return;
        }

        Order order = orders.findById(event.orderId()).orElse(null);
        if (order == null) {
            log.warn("Payment result {} references unknown order {}", event.eventId(), event.orderId());
            processed.save(new ProcessedResult(event.eventId()));
            return;
        }

        if (order.isPending()) {
            if (event.status() == PaymentResultEvent.PaymentStatus.APPROVED) {
                order.markConfirmed();
                log.info("Order {} CONFIRMED", order.getId());
            } else {
                order.markCancelled(event.reason());
                log.info("Order {} CANCELLED: {}", order.getId(), event.reason());
            }
        }
        processed.save(new ProcessedResult(event.eventId()));
    }
}
