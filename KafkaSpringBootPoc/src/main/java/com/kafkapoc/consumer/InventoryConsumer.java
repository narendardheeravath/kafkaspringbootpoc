package com.kafkapoc.consumer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

/**
 * SCENARIO 3 — Multiple Subscriber Groups (CQRS / Independent Processing)
 *
 * Two independent consumer groups both subscribe to topic-inventory.
 * Both groups receive EVERY inventory event independently — classic CQRS pattern:
 *   - Command side: warehouse updates physical stock levels
 *   - Query side:  reporting/analytics records the event for dashboards
 *
 * topic-inventory: 6 partitions, 1 replica.
 *
 * Groups:
 *   warehouse-group  → updates WMS (Warehouse Management System)
 *   reporting-group  → records for analytics / BI dashboards
 *
 * Log prefixes: [WAREHOUSE], [REPORTING]
 */
@Slf4j
@Service
public class InventoryConsumer {

    // ── Group 1: Warehouse Management System ──────────────────────────────────
    @KafkaListener(
            topics   = "${app.kafka.topics.inventory}",
            groupId  = "warehouse-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void processWarehouseInventory(
            @Payload  String message,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int  partition,
            @Header(KafkaHeaders.OFFSET)             long offset) {
        log.info("[WAREHOUSE]  partition={} offset={} | 🏭 Updating warehouse stock: {}", partition, offset, message);
    }

    // ── Group 2: Reporting & Analytics ────────────────────────────────────────
    @KafkaListener(
            topics   = "${app.kafka.topics.inventory}",
            groupId  = "reporting-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void processReportingInventory(
            @Payload  String message,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int  partition,
            @Header(KafkaHeaders.OFFSET)             long offset) {
        log.info("[REPORTING]  partition={} offset={} | 📊 Recording inventory for analytics: {}", partition, offset, message);
    }
}
