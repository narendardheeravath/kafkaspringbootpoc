package com.kafkapoc.consumer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

/**
 * SCENARIO 1 — Single Subscriber
 *
 * One consumer group (order-processor-group) with concurrency = 1.
 * Every message published to topic-orders is processed by exactly this
 * one consumer thread, in order within each partition.
 *
 * topic-orders: 3 partitions → 1 consumer thread handles all 3 partitions.
 *
 * Log prefix: [ORDER-CONSUMER]
 */
@Slf4j
@Service
public class OrderConsumer {

    @KafkaListener(
            topics     = "${app.kafka.topics.orders}",
            groupId    = "order-processor-group",
            concurrency = "1",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void processOrder(
            @Payload  String message,
            @Header(KafkaHeaders.RECEIVED_TOPIC)     String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int    partition,
            @Header(KafkaHeaders.OFFSET)             long   offset) {

        log.info("[ORDER-CONSUMER] ▶ topic={} partition={} offset={} | Received: {}",
                topic, partition, offset, message);

        // Simulate order processing (e.g. DB write, payment check)
        simulateWork(120);

        log.info("[ORDER-CONSUMER] ✔ Order processed: {}", message);
    }

    private void simulateWork(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }
}
