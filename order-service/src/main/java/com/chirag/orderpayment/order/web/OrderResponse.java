package com.chirag.orderpayment.order.web;

import com.chirag.orderpayment.order.domain.Order;
import com.chirag.orderpayment.order.domain.OrderStatus;

import java.math.BigDecimal;
import java.time.Instant;

public record OrderResponse(
        String id,
        String customerId,
        BigDecimal amount,
        String currency,
        OrderStatus status,
        String statusReason,
        Instant createdAt
) {
    public static OrderResponse from(Order order) {
        return new OrderResponse(
                order.getId(),
                order.getCustomerId(),
                order.getAmount(),
                order.getCurrency(),
                order.getStatus(),
                order.getStatusReason(),
                order.getCreatedAt());
    }
}
