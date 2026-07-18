package com.chirag.orderpayment.common;

/**
 * Header / MDC key names used to trace a single logical request as it hops
 * REST -> Postgres outbox -> Kafka -> Payment Service -> Kafka -> Order Service.
 */
public final class Headers {

    private Headers() {
    }

    /** Correlation id, carried on HTTP requests, Kafka records, and log MDC. */
    public static final String CORRELATION_ID = "X-Correlation-Id";

    /** MDC key (lower-case, no prefix) used in log patterns. */
    public static final String CORRELATION_ID_MDC = "correlationId";

    /** Kafka header carrying the business event id, used for idempotent consumption. */
    public static final String EVENT_ID = "event-id";
}
