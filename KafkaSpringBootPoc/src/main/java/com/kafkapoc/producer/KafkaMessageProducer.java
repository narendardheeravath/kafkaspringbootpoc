package com.kafkapoc.producer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Kafka message producer — wraps KafkaTemplate for all five topics.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaMessageProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;

    @Value("${app.kafka.topics.orders}")
    private String ordersTopic;

    @Value("${app.kafka.topics.notifications}")
    private String notificationsTopic;

    @Value("${app.kafka.topics.inventory}")
    private String inventoryTopic;

    @Value("${app.kafka.topics.audit}")
    private String auditTopic;

    @Value("${app.kafka.topics.payment}")
    private String paymentTopic;

    public void sendOrder(String key, String message) {
        send(ordersTopic, key, message);
    }

    public void sendNotification(String key, String message) {
        send(notificationsTopic, key, message);
    }

    public void sendInventory(String key, String message) {
        send(inventoryTopic, key, message);
    }

    public void sendAudit(String key, String message) {
        send(auditTopic, key, message);
    }

    public void sendPayment(String key, String message) {
        send(paymentTopic, key, message);
    }

    public void sendBulk(String topic, List<String> messages, String keyPrefix) {
        for (int i = 0; i < messages.size(); i++) {
            String key = (keyPrefix != null && !keyPrefix.isBlank()) ? keyPrefix + "-" + i : null;
            send(topic, key, messages.get(i));
        }
    }

    private void send(String topic, String key, String message) {
        CompletableFuture<SendResult<String, String>> future =
                (key != null) ? kafkaTemplate.send(topic, key, message)
                              : kafkaTemplate.send(topic, message);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("[PRODUCER] FAILED topic={} key={} error={}", topic, key, ex.getMessage());
            } else {
                log.info("[PRODUCER] SENT topic={} partition={} offset={} key={}",
                        topic,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset(),
                        key);
            }
        });
    }
}
