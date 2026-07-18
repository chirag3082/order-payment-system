package com.chirag.orderpayment.payment.messaging;

import com.chirag.orderpayment.common.Headers;
import com.chirag.orderpayment.common.Topics;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.RecordInterceptor;
import org.springframework.util.backoff.FixedBackOff;

import java.nio.charset.StandardCharsets;

/**
 * Configures the order-events consumer with:
 *  - retry with fixed backoff, then dead-letter to {@code order.events.DLT} for poison messages;
 *  - correlation-id propagation from the record into the log MDC.
 */
@Configuration
public class KafkaConsumerConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerConfig.class);

    @Bean
    public DeadLetterPublishingRecoverer deadLetterPublishingRecorder(KafkaTemplate<String, String> template) {
        // Route every failed record to a single DLT partitionless topic.
        return new DeadLetterPublishingRecoverer(template,
                (record, ex) -> new TopicPartition(Topics.ORDER_EVENTS_DLT, -1));
    }

    @Bean
    public DefaultErrorHandler errorHandler(DeadLetterPublishingRecoverer recorder) {
        // Retry 3 times, 1s apart, then publish to the DLT.
        DefaultErrorHandler handler = new DefaultErrorHandler(recorder, new FixedBackOff(1000L, 3L));
        handler.addNotRetryableExceptions(com.fasterxml.jackson.core.JsonProcessingException.class);
        return handler;
    }

    @Bean
    public RecordInterceptor<Object, Object> correlationRecordInterceptor() {
        return new RecordInterceptor<>() {
            @Override
            public ConsumerRecord<Object, Object> intercept(ConsumerRecord<Object, Object> record,
                                                            Consumer<Object, Object> consumer) {
                Header header = record.headers().lastHeader(Headers.CORRELATION_ID);
                if (header != null) {
                    MDC.put(Headers.CORRELATION_ID_MDC, new String(header.value(), StandardCharsets.UTF_8));
                }
                return record;
            }

            @Override
            public void afterRecord(ConsumerRecord<Object, Object> record, Consumer<Object, Object> consumer) {
                MDC.remove(Headers.CORRELATION_ID_MDC);
            }
        };
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<Object, Object> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory<Object, Object> consumerFactory,
            DefaultErrorHandler errorHandler,
            RecordInterceptor<Object, Object> recordInterceptor) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, consumerFactory);
        factory.setCommonErrorHandler(errorHandler);
        factory.setRecordInterceptor(recordInterceptor);
        return factory;
    }
}
