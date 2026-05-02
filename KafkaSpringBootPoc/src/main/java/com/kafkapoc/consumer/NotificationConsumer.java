package com.kafkapoc.consumer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

/**
 * SCENARIO 2 — Multiple Independent Subscribers (Broadcast / Fanout Pattern)
 *
 * Three separate consumer groups all subscribe to topic-notifications.
 * Because each group maintains its own independent offset cursor, EVERY
 * published message is delivered to ALL THREE groups — a broadcast/fanout.
 *
 * Use case: one "UserRegistered" event triggers push, email, AND SMS simultaneously.
 *
 * Groups:
 *   mobile-notification-group  → sends mobile push notification
 *   email-notification-group   → sends email
 *   sms-notification-group     → sends SMS
 *
 * Log prefixes: [MOBILE-NOTIF], [EMAIL-NOTIF], [SMS-NOTIF]
 */
@Slf4j
@Service
public class NotificationConsumer {

    // ── Group 1: Mobile push notifications ────────────────────────────────────
    @KafkaListener(
            topics   = "${app.kafka.topics.notifications}",
            groupId  = "mobile-notification-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void processMobileNotification(
            @Payload  String message,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int  partition,
            @Header(KafkaHeaders.OFFSET)             long offset) {
        log.info("[MOBILE-NOTIF] partition={} offset={} | 📱 Sending push notification: {}", partition, offset, message);
    }

    // ── Group 2: Email notifications ──────────────────────────────────────────
    @KafkaListener(
            topics   = "${app.kafka.topics.notifications}",
            groupId  = "email-notification-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void processEmailNotification(
            @Payload  String message,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int  partition,
            @Header(KafkaHeaders.OFFSET)             long offset) {
        log.info("[EMAIL-NOTIF]  partition={} offset={} | ✉️  Sending email: {}", partition, offset, message);
    }

    // ── Group 3: SMS notifications ────────────────────────────────────────────
    @KafkaListener(
            topics   = "${app.kafka.topics.notifications}",
            groupId  = "sms-notification-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void processSmsNotification(
            @Payload  String message,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int  partition,
            @Header(KafkaHeaders.OFFSET)             long offset) {
        log.info("[SMS-NOTIF]    partition={} offset={} | 💬 Sending SMS: {}", partition, offset, message);
    }
}
