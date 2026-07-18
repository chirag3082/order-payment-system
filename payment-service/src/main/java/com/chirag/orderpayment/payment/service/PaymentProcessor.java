package com.chirag.orderpayment.payment.service;

import com.chirag.orderpayment.common.event.OrderPlacedEvent;
import com.chirag.orderpayment.common.event.PaymentResultEvent;
import com.chirag.orderpayment.common.event.PaymentResultEvent.PaymentStatus;
import com.chirag.orderpayment.payment.domain.Payment;
import com.chirag.orderpayment.payment.domain.ProcessedEvent;
import com.chirag.orderpayment.payment.repo.PaymentRepository;
import com.chirag.orderpayment.payment.repo.ProcessedEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * Core payment logic. Idempotent by construction: the order event id is recorded
 * in a ledger under a primary-key constraint, so a duplicate delivery re-emits the
 * already-computed result rather than charging twice.
 */
@Service
public class PaymentProcessor {

    private static final Logger log = LoggerFactory.getLogger(PaymentProcessor.class);

    /** Amount used in demos/tests to force a processing failure (exercises retry + DLT). */
    private static final BigDecimal POISON_AMOUNT = new BigDecimal("6.66");

    private final PaymentRepository payments;
    private final ProcessedEventRepository processed;

    @Value("${payment.approval-limit:10000.00}")
    private BigDecimal approvalLimit;

    public PaymentProcessor(PaymentRepository payments, ProcessedEventRepository processed) {
        this.payments = payments;
        this.processed = processed;
    }

    @Transactional
    public PaymentResultEvent process(OrderPlacedEvent order) {
        if (processed.existsById(order.eventId())) {
            Payment existing = payments.findFirstByOrderId(order.orderId()).orElseThrow();
            log.debug("Duplicate order event {}; re-emitting stored result", order.eventId());
            return toResult(existing);
        }

        if (POISON_AMOUNT.compareTo(order.amount()) == 0) {
            // Simulated downstream failure: bubble up so the error handler retries
            // and eventually dead-letters the record.
            throw new PaymentProcessingException("Simulated payment gateway failure for order " + order.orderId());
        }

        boolean approved = order.amount().compareTo(approvalLimit) <= 0;
        Payment payment = new Payment(
                UUID.randomUUID().toString(),
                order.orderId(),
                order.amount(),
                order.currency(),
                approved ? PaymentStatus.APPROVED : PaymentStatus.DECLINED,
                approved ? null : "amount exceeds approval limit");
        payments.save(payment);
        processed.save(new ProcessedEvent(order.eventId(), order.orderId()));

        log.info("Order {} payment {}", order.orderId(), payment.getStatus());
        return toResult(payment);
    }

    private PaymentResultEvent toResult(Payment payment) {
        return new PaymentResultEvent(
                UUID.randomUUID().toString(),
                payment.getOrderId(),
                payment.getId(),
                payment.getStatus(),
                payment.getReason(),
                Instant.now());
    }
}
