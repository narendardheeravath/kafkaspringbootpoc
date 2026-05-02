package com.kafkapoc.consumer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

/**
 * SCENARIO 4 — Competing Consumers (Same Group, Multiple Concurrent Instances)
 *
 * A single consumer group (audit-group) uses concurrency = 3, which creates
 * 3 consumer threads within the same JVM process. Because they share the same
 * group ID, Kafka assigns partitions across them:
 *
 *   topic-audit has 6 partitions → 3 threads each own 2 partitions
 *
 * Messages are LOAD-BALANCED across threads — no message is processed twice.
 * This simulates horizontal scaling: more concurrency (or more app instances)
 * → higher throughput.
 *
 * To observe the effect: use POST /api/kafka/audit/bulk with 10+ messages.
 * Watch the [AUDIT-GROUP] log lines — different thread names handle different partitions.
 *
 * Log prefix: [AUDIT-GROUP]
 */
@Slf4j
@Service
public class AuditLogConsumer {

    @KafkaListener(
            topics      = "${app.kafka.topics.audit}",
            groupId     = "audit-group",
            concurrency = "3",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void processAuditLog(
            @Payload  String message,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int  partition,
            @Header(KafkaHeaders.OFFSET)             long offset) {

        log.info("[AUDIT-GROUP] thread={} partition={} offset={} | 📝 Persisting audit event: {}",
                Thread.currentThread().getName(), partition, offset, message);

        // Simulate a short DB write
        simulateWork(50);

        log.info("[AUDIT-GROUP] thread={} partition={} | ✔ Audit persisted: {}",
                Thread.currentThread().getName(), partition, message);
    }

    private void simulateWork(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }
}
