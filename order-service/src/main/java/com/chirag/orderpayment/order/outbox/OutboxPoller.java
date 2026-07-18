package com.chirag.orderpayment.order.outbox;

import com.chirag.orderpayment.common.Headers;
import com.chirag.orderpayment.order.domain.OutboxEvent;
import com.chirag.orderpayment.order.repo.OutboxRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.PageRequest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Relays committed outbox rows to Kafka. Runs on a fixed schedule, claims a batch
 * of unpublished rows with a row lock (SKIP LOCKED so multiple instances don't
 * collide), publishes each to Kafka, and marks it published — all in one
 * transaction. If the Kafka send or the DB update fails, the transaction rolls
 * back and the rows are retried on the next tick: at-least-once delivery.
 */
@Component
public class OutboxPoller {

    private static final Logger log = LoggerFactory.getLogger(OutboxPoller.class);

    private final OutboxRepository outbox;
    private final KafkaTemplate<String, String> kafka;
    private final Counter publishedCounter;

    @Value("${outbox.batch-size:100}")
    private int batchSize;

    public OutboxPoller(OutboxRepository outbox, KafkaTemplate<String, String> kafka, MeterRegistry meters) {
        this.outbox = outbox;
        this.kafka = kafka;
        this.publishedCounter = Counter.builder("outbox.events.published")
                .description("Outbox events relayed to Kafka")
                .register(meters);
    }

    @Scheduled(fixedDelayString = "${outbox.poll-interval-ms:1000}")
    @Transactional
    public void relayBatch() {
        List<OutboxEvent> batch = outbox.findUnpublishedForUpdate(PageRequest.of(0, batchSize));
        if (batch.isEmpty()) {
            return;
        }
        log.debug("Relaying {} outbox event(s) to Kafka", batch.size());
        for (OutboxEvent event : batch) {
            publish(event);
            event.markPublished();
            publishedCounter.increment();
        }
    }

    private void publish(OutboxEvent event) {
        ProducerRecord<String, String> record = new ProducerRecord<>(
                event.getTopic(), event.getAggregateId(), event.getPayload());
        record.headers().add(new RecordHeader(Headers.EVENT_ID,
                event.getId().getBytes(StandardCharsets.UTF_8)));
        record.headers().add(new RecordHeader(Headers.CORRELATION_ID,
                event.getCorrelationId().getBytes(StandardCharsets.UTF_8)));
        try {
            // Block for the broker ack so we only mark published on a durable write.
            kafka.send(record).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new OutboxPublishException(event.getId(), e);
        } catch (ExecutionException e) {
            throw new OutboxPublishException(event.getId(), e);
        }
    }
}
