package com.kafkapoc.controller;

import com.kafkapoc.model.BulkMessageRequest;
import com.kafkapoc.model.MessageRequest;
import com.kafkapoc.model.MessageResponse;
import com.kafkapoc.producer.KafkaMessageProducer;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * REST controller exposing all Kafka producer endpoints and scenario demonstrations.
 *
 * Swagger UI: http://localhost:8096/swagger-ui.html
 * Kafka UI:   http://localhost:8090
 */
@RestController
@RequestMapping("/api/kafka")
@RequiredArgsConstructor
@Tag(name = "Kafka Producer API",
        description = "Publish messages to Kafka topics demonstrating 5 consumer patterns")
public class KafkaController {

    private final KafkaMessageProducer producer;

    @Value("${app.kafka.topics.orders}")        private String ordersTopic;
    @Value("${app.kafka.topics.notifications}") private String notificationsTopic;
    @Value("${app.kafka.topics.inventory}")     private String inventoryTopic;
    @Value("${app.kafka.topics.audit}")         private String auditTopic;
    @Value("${app.kafka.topics.payment}")       private String paymentTopic;

    // ── Health ────────────────────────────────────────────────────────────────

    @GetMapping("/health")
    @Operation(summary = "Health check", description = "Returns service status and Kafka UI URL")
    public ResponseEntity<Map<String, String>> health() {
        return ResponseEntity.ok(Map.of(
                "status",    "UP",
                "service",   "KafkaSpringBootPoc",
                "port",      "8096",
                "swaggerUi", "http://localhost:8096/swagger-ui.html",
                "kafkaUi",   "http://localhost:8090"
        ));
    }

    // ── Topic Info ────────────────────────────────────────────────────────────

    @GetMapping("/topics/info")
    @Operation(
            summary = "Topic & Consumer Group Configuration",
            description = "Lists all 5 configured topics with partition counts, scenarios, and consumer groups."
    )
    public ResponseEntity<Map<String, Object>> topicsInfo() {
        return ResponseEntity.ok(Map.of(
                "topics", List.of(
                        Map.of("topic", ordersTopic,        "partitions", 3,
                               "scenario", "Single Subscriber",
                               "consumerGroups", List.of("order-processor-group")),
                        Map.of("topic", notificationsTopic, "partitions", 3,
                               "scenario", "Multiple Subscribers — Broadcast (3 groups each receive every message)",
                               "consumerGroups", List.of("mobile-notification-group", "email-notification-group", "sms-notification-group")),
                        Map.of("topic", inventoryTopic,     "partitions", 6,
                               "scenario", "Multiple Groups — CQRS (2 groups process independently)",
                               "consumerGroups", List.of("warehouse-group", "reporting-group")),
                        Map.of("topic", auditTopic,         "partitions", 6,
                               "scenario", "Competing Consumers (same group, concurrency=3)",
                               "consumerGroups", List.of("audit-group (3 threads)")),
                        Map.of("topic", paymentTopic,       "partitions", 3,
                               "scenario", "Batch Consumer (messages collected and processed in batches)",
                               "consumerGroups", List.of("payment-batch-group"))
                ),
                "swaggerUi", "http://localhost:8096/swagger-ui.html",
                "kafkaUi",   "http://localhost:8090"
        ));
    }

    // ── Scenario 1: Single Subscriber ────────────────────────────────────────

    @PostMapping("/orders/send")
    @Operation(
            summary     = "SCENARIO 1 — Single Subscriber",
            description = """
                    Publishes one message to **topic-orders** (3 partitions).

                    **Pattern:** Single Subscriber
                    - Consumer group: `order-processor-group`
                    - Concurrency: `1` — one thread processes all messages sequentially
                    - Every message is handled by exactly ONE consumer instance

                    **Watch logs for:** `[ORDER-CONSUMER]`

                    **Sample message:** `Order #001: 2x Laptop, customer: Alice`
                    """
    )
    public ResponseEntity<MessageResponse> sendOrder(@Valid @RequestBody MessageRequest request) {
        producer.sendOrder(request.getKey(), request.getMessage());
        return ResponseEntity.ok(MessageResponse.success(
                ordersTopic,
                "Single Subscriber — one consumer group processes all messages",
                List.of("order-processor-group"),
                1
        ));
    }

    // ── Scenario 2: Multiple Subscribers (Broadcast) ─────────────────────────

    @PostMapping("/notifications/send")
    @Operation(
            summary     = "SCENARIO 2 — Multiple Subscribers (Broadcast)",
            description = """
                    Publishes one message to **topic-notifications** (3 partitions).

                    **Pattern:** Multiple Independent Subscribers — Broadcast / Fanout
                    - Group 1: `mobile-notification-group` → sends push notification
                    - Group 2: `email-notification-group`  → sends email
                    - Group 3: `sms-notification-group`    → sends SMS

                    Each group maintains its own offset cursor → **ALL 3 groups receive every message**.

                    **Watch logs for:** `[MOBILE-NOTIF]`, `[EMAIL-NOTIF]`, `[SMS-NOTIF]`

                    **Sample message:** `User registered: bob@example.com`
                    """
    )
    public ResponseEntity<MessageResponse> sendNotification(@Valid @RequestBody MessageRequest request) {
        producer.sendNotification(request.getKey(), request.getMessage());
        return ResponseEntity.ok(MessageResponse.success(
                notificationsTopic,
                "Multiple Subscribers (Broadcast) — 3 independent groups each receive every message",
                List.of("mobile-notification-group", "email-notification-group", "sms-notification-group"),
                1
        ));
    }

    // ── Scenario 3: Multiple Groups (CQRS) ───────────────────────────────────

    @PostMapping("/inventory/send")
    @Operation(
            summary     = "SCENARIO 3 — Multiple Subscriber Groups (CQRS Pattern)",
            description = """
                    Publishes one message to **topic-inventory** (6 partitions).

                    **Pattern:** Multiple Independent Groups — CQRS event fanout
                    - Group 1: `warehouse-group`  → updates physical warehouse stock (command side)
                    - Group 2: `reporting-group`  → records analytics data (query side)

                    Both groups independently consume every inventory event.

                    **Watch logs for:** `[WAREHOUSE]`, `[REPORTING]`

                    **Sample message:** `SKU-789: stock -5 units, warehouse: London`
                    """
    )
    public ResponseEntity<MessageResponse> sendInventory(@Valid @RequestBody MessageRequest request) {
        producer.sendInventory(request.getKey(), request.getMessage());
        return ResponseEntity.ok(MessageResponse.success(
                inventoryTopic,
                "Multiple Groups (CQRS) — warehouse and reporting groups each receive every event",
                List.of("warehouse-group", "reporting-group"),
                1
        ));
    }

    // ── Scenario 4a: Competing Consumers (single message) ────────────────────

    @PostMapping("/audit/send")
    @Operation(
            summary     = "SCENARIO 4 — Competing Consumers (Single Message)",
            description = """
                    Publishes one message to **topic-audit** (6 partitions).

                    **Pattern:** Competing Consumers — same group, multiple concurrent instances
                    - Group: `audit-group` with `concurrency = 3` (3 consumer threads)
                    - 6 partitions ÷ 3 threads = each thread owns 2 partitions
                    - Messages are **load-balanced** across threads — no duplicates

                    Use `/audit/bulk` to send many messages and observe different threads processing them.

                    **Watch logs for:** `[AUDIT-GROUP]` — note varying thread names!

                    **Sample message:** `USER_LOGIN: user=alice, ip=192.168.1.1, ts=2026-04-29T10:00:00Z`
                    """
    )
    public ResponseEntity<MessageResponse> sendAudit(@Valid @RequestBody MessageRequest request) {
        producer.sendAudit(request.getKey(), request.getMessage());
        return ResponseEntity.ok(MessageResponse.success(
                auditTopic,
                "Competing Consumers — same group, concurrency=3, messages load-balanced across threads",
                List.of("audit-group (3 threads)"),
                1
        ));
    }

    // ── Scenario 4b: Competing Consumers (bulk) ───────────────────────────────

    @PostMapping("/audit/bulk")
    @Operation(
            summary     = "SCENARIO 4b — Bulk Audit (observe competing consumers in action)",
            description = """
                    Sends multiple messages to **topic-audit** in one request.

                    With 6 partitions and 3 concurrent consumer threads, you will see in the logs
                    that **different threads process messages from different partitions simultaneously**.

                    This demonstrates horizontal scaling — increase `concurrency` to handle more load.

                    **Sample body:**
                    ```json
                    {
                      "keyPrefix": "audit",
                      "messages": [
                        "USER_LOGIN: alice", "ORDER_PLACED: #100", "PAYMENT_PROCESSED: txn-555",
                        "USER_LOGOUT: bob",  "ITEM_VIEWED: SKU-42", "CART_UPDATED: user=carol",
                        "USER_LOGIN: dave",  "ORDER_SHIPPED: #98", "REFUND_ISSUED: txn-301",
                        "USER_SIGNUP: eve"
                      ]
                    }
                    ```
                    """
    )
    public ResponseEntity<MessageResponse> sendBulkAudit(@Valid @RequestBody BulkMessageRequest request) {
        producer.sendBulk(auditTopic, request.getMessages(), request.getKeyPrefix());
        return ResponseEntity.ok(MessageResponse.success(
                auditTopic,
                "Bulk Competing Consumers — observe multiple threads processing partitions in parallel",
                List.of("audit-group (3 threads)"),
                request.getMessages().size()
        ));
    }

    // ── Scenario 5a: Batch Consumer (single message) ──────────────────────────

    @PostMapping("/payments/send")
    @Operation(
            summary     = "SCENARIO 5 — Batch Consumer (Single Message)",
            description = """
                    Publishes one message to **topic-payment** (3 partitions).

                    **Pattern:** Batch Consumer
                    - Group: `payment-batch-group`
                    - Uses `batchKafkaListenerContainerFactory` (setBatchListener = true)
                    - Consumer polls and processes **multiple messages per call** for efficiency

                    Use `/payments/bulk` to send many messages and observe batch sizes > 1.

                    **Watch logs for:** `[PAYMENT-BATCH]` — note "Received batch of N payments"

                    **Sample message:** `Payment txn-001: $150.00, card: **** 4242, merchant: Amazon`
                    """
    )
    public ResponseEntity<MessageResponse> sendPayment(@Valid @RequestBody MessageRequest request) {
        producer.sendPayment(request.getKey(), request.getMessage());
        return ResponseEntity.ok(MessageResponse.success(
                paymentTopic,
                "Batch Consumer — messages collected per poll and processed together",
                List.of("payment-batch-group"),
                1
        ));
    }

    // ── Scenario 5b: Batch Consumer (bulk messages) ───────────────────────────

    @PostMapping("/payments/bulk")
    @Operation(
            summary     = "SCENARIO 5b — Bulk Payments (observe batch consumer in action)",
            description = """
                    Sends multiple payment messages to **topic-payment** at once.

                    The batch consumer collects all available messages in a single `poll()` call.
                    Watch the `[PAYMENT-BATCH]` logs: "Received batch of N payments" shows batching.

                    **Sample body:**
                    ```json
                    {
                      "keyPrefix": "pay",
                      "messages": [
                        "txn-001: $150.00 Amazon",
                        "txn-002: $89.99  Netflix",
                        "txn-003: $220.00 Apple",
                        "txn-004: $45.50  Spotify",
                        "txn-005: $300.00 Microsoft",
                        "txn-006: $12.99  Adobe"
                      ]
                    }
                    ```
                    """
    )
    public ResponseEntity<MessageResponse> sendBulkPayments(@Valid @RequestBody BulkMessageRequest request) {
        producer.sendBulk(paymentTopic, request.getMessages(), request.getKeyPrefix());
        return ResponseEntity.ok(MessageResponse.success(
                paymentTopic,
                "Batch Consumer — observe batch of N in [PAYMENT-BATCH] logs",
                List.of("payment-batch-group"),
                request.getMessages().size()
        ));
    }
}
