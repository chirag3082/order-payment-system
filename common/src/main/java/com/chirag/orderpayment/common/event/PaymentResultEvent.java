package com.chirag.orderpayment.common.event;

import java.time.Instant;

/**
 * Emitted by the Payment Service after attempting to charge an order.
 * Consumed by the Order Service to complete the choreography saga by
 * confirming or cancelling the order.
 */
public record PaymentResultEvent(
        String eventId,
        String orderId,
        String paymentId,
        PaymentStatus status,
        String reason,
        Instant occurredAt
) {
    public enum PaymentStatus {
        APPROVED,
        DECLINED
    }
}
