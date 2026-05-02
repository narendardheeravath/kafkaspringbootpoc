package com.kafkapoc.consumer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * SCENARIO 5 — Batch Consumer
 *
 * Instead of processing one record at a time, the batch container factory
 * collects multiple records in a single poll() call and passes them as a List.
 *
 * Uses {@code batchKafkaListenerContainerFactory} (setBatchListener = true).
 *
 * Advantages:
 *   - Reduces DB round-trips (bulk insert instead of individual inserts)
 *   - Higher throughput for payment/transaction processing
 *   - Amortises network/serialisation overhead
 *
 * To observe batching: use POST /api/kafka/payments/bulk with 10+ messages.
 * The log will show "Received batch of N payments".
 *
 * Log prefix: [PAYMENT-BATCH]
 */
@Slf4j
@Service
public class PaymentConsumer {

    @KafkaListener(
            topics   = "${app.kafka.topics.payment}",
            groupId  = "payment-batch-group",
            containerFactory = "batchKafkaListenerContainerFactory"
    )
    public void processPaymentBatch(
            @Payload List<String>  messages,
            @Header(KafkaHeaders.RECEIVED_PARTITION) List<Integer> partitions,
            @Header(KafkaHeaders.OFFSET)             List<Long>    offsets) {

        log.info("[PAYMENT-BATCH] ▶ Received batch of {} payment(s)", messages.size());

        for (int i = 0; i < messages.size(); i++) {
            log.info("[PAYMENT-BATCH]   [{}/{}] partition={} offset={} | 💳 Processing: {}",
                    i + 1, messages.size(),
                    partitions.get(i), offsets.get(i),
                    messages.get(i));
        }

        // Simulate bulk DB insert for the entire batch
        simulateWork(messages.size() * 20L);

        log.info("[PAYMENT-BATCH] ✔ Batch of {} payment(s) committed to DB.", messages.size());
    }

    private void simulateWork(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }
}
