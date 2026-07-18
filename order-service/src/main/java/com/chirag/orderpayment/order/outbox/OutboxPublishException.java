package com.chirag.orderpayment.order.outbox;

public class OutboxPublishException extends RuntimeException {
    public OutboxPublishException(String outboxId, Throwable cause) {
        super("Failed to publish outbox event " + outboxId, cause);
    }
}
