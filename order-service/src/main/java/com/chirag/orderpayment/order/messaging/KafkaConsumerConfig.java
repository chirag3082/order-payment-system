package com.chirag.orderpayment.order.messaging;

import com.chirag.orderpayment.common.Headers;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.MDC;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.RecordInterceptor;

import java.nio.charset.StandardCharsets;

/**
 * Propagates the correlation id carried on a Kafka record into the SLF4J MDC for
 * the duration of the listener invocation, so a single logical request can be
 * traced across the async boundary between services.
 */
@Configuration
public class KafkaConsumerConfig {

    @Bean
    public RecordInterceptor<String, String> correlationRecordInterceptor() {
        return new RecordInterceptor<>() {
            @Override
            public ConsumerRecord<String, String> intercept(ConsumerRecord<String, String> record,
                                                             Consumer<String, String> consumer) {
                Header header = record.headers().lastHeader(Headers.CORRELATION_ID);
                if (header != null) {
                    MDC.put(Headers.CORRELATION_ID_MDC, new String(header.value(), StandardCharsets.UTF_8));
                }
                return record;
            }

            @Override
            public void afterRecord(ConsumerRecord<String, String> record, Consumer<String, String> consumer) {
                MDC.remove(Headers.CORRELATION_ID_MDC);
            }
        };
    }
}
