package com.chirag.orderpayment.order.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;

/**
 * Idempotency ledger for payment-result events consumed by the Order Service.
 * Inserting the event id under a primary-key/unique constraint makes duplicate
 * deliveries (at-least-once) a no-op.
 */
@Entity
@Table(name = "processed_payment_results")
public class ProcessedResult {

    @Id
    @Column(name = "event_id", nullable = false, updatable = false)
    private String eventId;

    @Column(nullable = false, updatable = false)
    private Instant processedAt;

    protected ProcessedResult() {
    }

    public ProcessedResult(String eventId) {
        this.eventId = eventId;
        this.processedAt = Instant.now();
    }

    public String getEventId() {
        return eventId;
    }
}
