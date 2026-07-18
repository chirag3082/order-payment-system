package com.chirag.orderpayment.order.service;

import com.chirag.orderpayment.common.Headers;
import com.chirag.orderpayment.common.Topics;
import com.chirag.orderpayment.common.event.OrderPlacedEvent;
import com.chirag.orderpayment.order.domain.Order;
import com.chirag.orderpayment.order.domain.OutboxEvent;
import com.chirag.orderpayment.order.repo.OrderRepository;
import com.chirag.orderpayment.order.repo.OutboxRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.MDC;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Service
public class OrderService {

    private final OrderRepository orders;
    private final OutboxRepository outbox;
    private final ObjectMapper objectMapper;

    public OrderService(OrderRepository orders, OutboxRepository outbox, ObjectMapper objectMapper) {
        this.orders = orders;
        this.outbox = outbox;
        this.objectMapper = objectMapper;
    }

    /**
     * Persists the order AND its "OrderPlaced" outbox event atomically. Because both
     * writes are in the same transaction, a crash either commits both or neither —
     * the outbox poller will later relay the event to Kafka. This is the core of the
     * dual-write fix.
     */
    @Transactional
    public Order placeOrder(String customerId, BigDecimal amount, String currency) {
        String orderId = UUID.randomUUID().toString();
        Order order = new Order(orderId, customerId, amount, currency);
        orders.save(order);

        String correlationId = currentCorrelationId();
        OrderPlacedEvent event = new OrderPlacedEvent(
                UUID.randomUUID().toString(),
                orderId,
                customerId,
                amount,
                currency,
                Instant.now());

        outbox.save(new OutboxEvent(
                event.eventId(),
                "order",
                orderId,
                "OrderPlaced",
                Topics.ORDER_EVENTS,
                serialize(event),
                correlationId));

        return order;
    }

    @Transactional(readOnly = true)
    public Order find(String orderId) {
        return orders.findById(orderId)
                .orElseThrow(() -> new OrderNotFoundException(orderId));
    }

    private String serialize(OrderPlacedEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize OrderPlacedEvent", e);
        }
    }

    private String currentCorrelationId() {
        String cid = MDC.get(Headers.CORRELATION_ID_MDC);
        return cid != null ? cid : UUID.randomUUID().toString();
    }
}
