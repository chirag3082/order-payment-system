package com.chirag.orderpayment.order.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;

/**
 * Transactional outbox row. Written in the SAME database transaction as the
 * business change (the order). A separate poller relays committed rows to Kafka,
 * so there is never a "dual write" where the DB commit and the Kafka publish
 * can diverge on a crash.
 */
@Entity
@Table(name = "outbox_events")
public class OutboxEvent {

    @Id
    @Column(nullable = false, updatable = false)
    private String id;

    /** e.g. "order" — the aggregate this event belongs to. */
    @Column(nullable = false)
    private String aggregateType;

    /** e.g. the order id — used as the Kafka partition key for ordering per aggregate. */
    @Column(nullable = false)
    private String aggregateId;

    /** e.g. "OrderPlaced". */
    @Column(nullable = false)
    private String eventType;

    /** Destination Kafka topic. */
    @Column(nullable = false)
    private String topic;

    /** JSON-serialized event body. */
    @Column(nullable = false, columnDefinition = "text")
    private String payload;

    @Column(nullable = false)
    private String correlationId;

    @Column(nullable = false, updatable = false)
    private Instant createdAt;

    @Column(nullable = false)
    private boolean published;

    private Instant publishedAt;

    protected OutboxEvent() {
        // for JPA
    }

    public OutboxEvent(String id, String aggregateType, String aggregateId, String eventType,
                       String topic, String payload, String correlationId) {
        this.id = id;
        this.aggregateType = aggregateType;
        this.aggregateId = aggregateId;
        this.eventType = eventType;
        this.topic = topic;
        this.payload = payload;
        this.correlationId = correlationId;
        this.createdAt = Instant.now();
        this.published = false;
    }

    public void markPublished() {
        this.published = true;
        this.publishedAt = Instant.now();
    }

    public String getId() {
        return id;
    }

    public String getAggregateId() {
        return aggregateId;
    }

    public String getEventType() {
        return eventType;
    }

    public String getTopic() {
        return topic;
    }

    public String getPayload() {
        return payload;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public boolean isPublished() {
        return published;
    }
}
