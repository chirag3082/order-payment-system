package com.chirag.orderpayment.common;

/**
 * Kafka topic names shared across services. Keeping them in one place makes the
 * event contract between Order and Payment services explicit and impossible to typo.
 */
public final class Topics {

    private Topics() {
    }

    /** Order Service publishes OrderPlaced events here; Payment Service consumes. */
    public static final String ORDER_EVENTS = "order.events";

    /** Payment Service publishes PaymentResult events here; Order Service consumes. */
    public static final String PAYMENT_RESULTS = "payment.results";

    /** Dead-letter topic for order events that fail processing after retries. */
    public static final String ORDER_EVENTS_DLT = "order.events.DLT";
}
