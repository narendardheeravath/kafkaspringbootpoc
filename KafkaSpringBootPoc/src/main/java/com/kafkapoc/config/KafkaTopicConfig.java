package com.kafkapoc.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Declares all Kafka topics.
 * Spring Boot auto-creates them in Kafka on startup via KafkaAdmin.
 *
 * Topics and partition counts:
 *   topic-orders        (3 partitions) — Scenario 1: Single Subscriber
 *   topic-notifications (3 partitions) — Scenario 2: Multiple Subscribers (Broadcast)
 *   topic-inventory     (6 partitions) — Scenario 3: Multiple Groups (CQRS)
 *   topic-audit         (6 partitions) — Scenario 4: Competing Consumers
 *   topic-payment       (3 partitions) — Scenario 5: Batch Consumer
 */
@Configuration
public class KafkaTopicConfig {

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

    @Bean
    public NewTopic topicOrders() {
        return TopicBuilder.name(ordersTopic).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic topicNotifications() {
        return TopicBuilder.name(notificationsTopic).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic topicInventory() {
        return TopicBuilder.name(inventoryTopic).partitions(6).replicas(1).build();
    }

    @Bean
    public NewTopic topicAudit() {
        return TopicBuilder.name(auditTopic).partitions(6).replicas(1).build();
    }

    @Bean
    public NewTopic topicPayment() {
        return TopicBuilder.name(paymentTopic).partitions(3).replicas(1).build();
    }
}
