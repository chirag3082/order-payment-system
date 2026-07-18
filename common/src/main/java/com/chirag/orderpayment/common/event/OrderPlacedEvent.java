package com.chirag.orderpayment.common.event;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Emitted by the Order Service when an order is accepted and persisted.
 * {@code eventId} uniquely identifies this event and is the key used by the
 * Payment Service to consume idempotently (dedupe on at-least-once delivery).
 */
public record OrderPlacedEvent(
        String eventId,
        String orderId,
        String customerId,
        BigDecimal amount,
        String currency,
        Instant occurredAt
) {
}
