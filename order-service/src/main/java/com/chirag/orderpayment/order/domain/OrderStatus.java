package com.chirag.orderpayment.order.domain;

public enum OrderStatus {
    /** Order accepted and persisted; payment not yet resolved. */
    PENDING,
    /** Payment approved; saga completed successfully. */
    CONFIRMED,
    /** Payment declined (or order rejected); saga completed with compensation. */
    CANCELLED
}
