package com.chirag.orderpayment.payment.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;

/**
 * Idempotency ledger for order events consumed by the Payment Service. The order
 * event id is the primary key, so a duplicate delivery collides on insert and is
 * skipped — this is what makes the consumer idempotent under at-least-once delivery.
 */
@Entity
@Table(name = "processed_events")
public class ProcessedEvent {

    @Id
    @Column(name = "event_id", nullable = false, updatable = false)
    private String eventId;

    @Column(nullable = false)
    private String orderId;

    @Column(nullable = false, updatable = false)
    private Instant processedAt;

    protected ProcessedEvent() {
    }

    public ProcessedEvent(String eventId, String orderId) {
        this.eventId = eventId;
        this.orderId = orderId;
        this.processedAt = Instant.now();
    }

    public String getEventId() {
        return eventId;
    }
}
