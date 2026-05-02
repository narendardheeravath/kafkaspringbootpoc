# KafkaSpringBootPoc

**Spring Boot 3.4.5 / Java 21 / Apache Kafka** POC demonstrating five consumer patterns:

| # | Pattern | Topic | Description |
|---|---|---|---|
| 1 | **Single Subscriber** | `topic-orders` | One group, one thread — sequential processing |
| 2 | **Multiple Subscribers (Broadcast)** | `topic-notifications` | 3 groups each get every message (fanout) |
| 3 | **Multiple Groups (CQRS)** | `topic-inventory` | 2 independent groups process all events |
| 4 | **Competing Consumers** | `topic-audit` | Same group, concurrency=3 — load balanced |
| 5 | **Batch Consumer** | `topic-payment` | Messages collected and processed in batches |

---

## Table of Contents

- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Step 1 — Start Kafka (Docker Compose)](#step-1--start-kafka-docker-compose)
- [Step 2 — Build & Run the App](#step-2--build--run-the-app)
- [Step 3 — Open Swagger UI](#step-3--open-swagger-ui)
- [API Endpoints](#api-endpoints)
- [Test Scenarios](#test-scenarios)
  - [Health Check](#health-check)
  - [Scenario 1 — Single Subscriber](#scenario-1--single-subscriber-topic-orders)
  - [Scenario 2 — Multiple Subscribers / Broadcast](#scenario-2--multiple-subscribers--broadcast-topic-notifications)
  - [Scenario 3 — Multiple Groups / CQRS](#scenario-3--multiple-groups--cqrs-topic-inventory)
  - [Scenario 4 — Competing Consumers](#scenario-4--competing-consumers-topic-audit)
  - [Scenario 5 — Batch Consumer](#scenario-5--batch-consumer-topic-payment)
- [Kafka UI Dashboard](#kafka-ui-dashboard)
- [Consumer Pattern Details](#consumer-pattern-details)
- [Project Structure](#project-structure)
- [Troubleshooting](#troubleshooting)

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        KafkaSpringBootPoc :8096                          │
│                                                                          │
│  KafkaController (REST/Swagger)                                          │
│       │                                                                  │
│       ▼                                                                  │
│  KafkaMessageProducer ──────────────────────────────────────────────┐   │
│  (KafkaTemplate)                                                     │   │
│                                                                      ▼   │
│  ┌──────────────────────────────────────────────────────────────────┐│  │
│  │                     Apache Kafka Broker :9092                     ││  │
│  │                                                                   ││  │
│  │  topic-orders        (3 partitions)  ──► order-processor-group   ││  │
│  │  topic-notifications (3 partitions)  ──► mobile-notification-group  │
│  │                                         email-notification-group  │  │
│  │                                         sms-notification-group   ││  │
│  │  topic-inventory     (6 partitions)  ──► warehouse-group         ││  │
│  │                                         reporting-group          ││  │
│  │  topic-audit         (6 partitions)  ──► audit-group (3 threads) ││  │
│  │  topic-payment       (3 partitions)  ──► payment-batch-group     ││  │
│  └──────────────────────────────────────────────────────────────────┘│  │
└─────────────────────────────────────────────────────────────────────────┘

Supporting services:
  ZooKeeper  :2181   (Kafka coordination)
  Kafka UI   :8090   (visual dashboard)
```

---

## Prerequisites

| Requirement | Version | Check |
|---|---|---|
| Java | 21+ | `java -version` |
| Maven | 3.9+ | `mvn -version` |
| Docker Desktop | Latest | `docker --version` |
| Docker Compose | v2+ | `docker compose version` |

---

## Step 1 — Start Kafka (Docker Compose)

```powershell
cd KafkaSpringBootPoc

# Start ZooKeeper + Kafka broker + Kafka UI
docker compose up -d

# Verify all 3 containers are running
docker compose ps
```

Expected output:
```
NAME         STATUS          PORTS
zookeeper    Up (healthy)    0.0.0.0:2181->2181/tcp
kafka        Up (healthy)    0.0.0.0:9092->9092/tcp
kafka-ui     Up              0.0.0.0:8090->8080/tcp
```

> **Tip:** Wait ~15 seconds for Kafka to become healthy before starting the app.

### Stop Kafka
```powershell
docker compose down          # stop (preserves data)
docker compose down -v       # stop + delete all volumes (clean slate)
```

---

## Step 2 — Build & Run the App

```powershell
cd KafkaSpringBootPoc
mvn spring-boot:run
```

Or build a JAR first:
```powershell
mvn clean package -DskipTests
java -jar target/kafka-spring-boot-poc-1.0.0.jar
```

**Expected startup output:**
```
[main] Started KafkaSpringBootPocApplication in 4.2 seconds
[main] Tomcat started on port 8096 (http)
```

The app will automatically create all 5 Kafka topics on startup via `KafkaTopicConfig`.

---

## Step 3 — Open Swagger UI

```
http://localhost:8096/swagger-ui.html
```

All endpoints are documented with scenario descriptions, sample payloads, and consumer group info.

---

## API Endpoints

| Method | Path | Scenario | Pattern |
|---|---|---|---|
| `GET`  | `/api/kafka/health` | — | Health check |
| `GET`  | `/api/kafka/topics/info` | — | Topic + consumer group configuration |
| `POST` | `/api/kafka/orders/send` | 1 | Single Subscriber |
| `POST` | `/api/kafka/notifications/send` | 2 | Multiple Subscribers (Broadcast) |
| `POST` | `/api/kafka/inventory/send` | 3 | Multiple Groups (CQRS) |
| `POST` | `/api/kafka/audit/send` | 4 | Competing Consumers (single) |
| `POST` | `/api/kafka/audit/bulk` | 4b | Competing Consumers (bulk — best for demo) |
| `POST` | `/api/kafka/payments/send` | 5 | Batch Consumer (single) |
| `POST` | `/api/kafka/payments/bulk` | 5b | Batch Consumer (bulk — best for demo) |

---

## Test Scenarios

> All scenarios below include the **actual HTTP input and output** captured from a live test run on 2026-04-29 against `http://localhost:8096`.

---

### Health Check

**Input:**
```powershell
Invoke-RestMethod "http://localhost:8096/api/kafka/health" | ConvertTo-Json
```

**Output:**
```json
{
  "service":  "KafkaSpringBootPoc",
  "status":   "UP",
  "swaggerUi": "http://localhost:8096/swagger-ui.html",
  "port":     "8096",
  "kafkaUi":  "http://localhost:8090"
}
```

---

### Scenario 1 — Single Subscriber (`topic-orders`)

One consumer group, one thread. Messages processed sequentially.

**Input:**
```powershell
Invoke-RestMethod "http://localhost:8096/api/kafka/orders/send" `
  -Method POST -ContentType "application/json" `
  -Body '{"key":"ord-001","message":"Order #001: 2x Laptop, customer: Alice"}' | ConvertTo-Json
```

**API Response:**
```json
{
  "status":        "SENT",
  "topic":         "topic-orders",
  "messageCount":  1,
  "scenario":      "Single Subscriber — one consumer group processes all messages",
  "consumerGroups": ["order-processor-group"],
  "sentAt":        "2026-04-29T14:22:05.998824800Z"
}
```

**Consumer Logs:**
```
[ORDER-CONSUMER] ▶ topic=topic-orders partition=1 offset=0 key=ord-001
                   | Received: Order #001: 2x Laptop, customer: Alice
[ORDER-CONSUMER] ✔ Order processed: Order #001: 2x Laptop, customer: Alice
```

**Consumer Pattern:**
```
topic-orders (3 partitions)
      │
      └── order-processor-group  (concurrency=1)
              └── Thread-1  ← processes P0, P1, P2 sequentially
```

---

### Scenario 2 — Multiple Subscribers / Broadcast (`topic-notifications`)

One message → three independent consumer groups each receive it.

**Input:**
```powershell
Invoke-RestMethod "http://localhost:8096/api/kafka/notifications/send" `
  -Method POST -ContentType "application/json" `
  -Body '{"key":"notif-001","message":"User registered: alice@example.com"}' | ConvertTo-Json
```

**API Response:**
```json
{
  "status":        "SENT",
  "topic":         "topic-notifications",
  "messageCount":  1,
  "scenario":      "Multiple Subscribers (Broadcast) — 3 independent groups each receive every message",
  "consumerGroups": [
    "mobile-notification-group",
    "email-notification-group",
    "sms-notification-group"
  ],
  "sentAt":        "2026-04-29T14:22:54.176549100Z"
}
```

**Consumer Logs (all 3 fire for every single message):**
```
[MOBILE-NOTIF] partition=1 offset=0 | 📱 Sending push notification: User registered: alice@example.com
[EMAIL-NOTIF]  partition=1 offset=0 | ✉️  Sending email: User registered: alice@example.com
[SMS-NOTIF]    partition=1 offset=0 | 💬 Sending SMS: User registered: alice@example.com
```

**Consumer Pattern:**
```
topic-notifications (3 partitions)
      │
      ├── mobile-notification-group  ← offset tracked independently → ALL messages
      ├── email-notification-group   ← offset tracked independently → ALL messages
      └── sms-notification-group     ← offset tracked independently → ALL messages
```

---

### Scenario 3 — Multiple Groups / CQRS (`topic-inventory`)

Two independent groups both process every inventory event for different purposes.

**Input:**
```powershell
Invoke-RestMethod "http://localhost:8096/api/kafka/inventory/send" `
  -Method POST -ContentType "application/json" `
  -Body '{"key":"inv-001","message":"SKU-789: stock -5 units, warehouse: London"}' | ConvertTo-Json
```

**API Response:**
```json
{
  "status":        "SENT",
  "topic":         "topic-inventory",
  "messageCount":  1,
  "scenario":      "Multiple Groups (CQRS) — warehouse and reporting groups each receive every event",
  "consumerGroups": [
    "warehouse-group",
    "reporting-group"
  ],
  "sentAt":        "2026-04-29T14:23:04.247605600Z"
}
```

**Consumer Logs (both groups fire for each message):**
```
[WAREHOUSE] partition=2 offset=0 | 🏭 Updating warehouse stock: SKU-789: stock -5 units, warehouse: London
[REPORTING] partition=2 offset=0 | 📊 Recording inventory for analytics: SKU-789: stock -5 units, warehouse: London
```

**Consumer Pattern:**
```
topic-inventory (6 partitions)
      │
      ├── warehouse-group  ← writes stock changes to WMS
      └── reporting-group  ← writes same events to analytics DB
```

---

### Scenario 4 — Competing Consumers (`topic-audit`)

Same group, 3 concurrent threads load-balance across 6 partitions. No message processed twice.

**Input (bulk — 12 messages):**
```powershell
$body = '{"keyPrefix":"audit","messages":["USER_LOGIN: alice","ORDER_PLACED: #100",
  "PAYMENT_PROCESSED: txn-555","USER_LOGOUT: bob","ITEM_VIEWED: SKU-42",
  "CART_UPDATED: carol","USER_LOGIN: dave","ORDER_SHIPPED: #98",
  "REFUND_ISSUED: txn-301","USER_SIGNUP: eve","LOGIN_FAILED: frank",
  "PROFILE_UPDATED: grace"]}'

Invoke-RestMethod "http://localhost:8096/api/kafka/audit/bulk" `
  -Method POST -ContentType "application/json" -Body $body | ConvertTo-Json
```

**API Response:**
```json
{
  "status":        "SENT",
  "topic":         "topic-audit",
  "messageCount":  12,
  "scenario":      "Bulk Competing Consumers — observe multiple threads processing partitions in parallel",
  "consumerGroups": ["audit-group (3 threads)"],
  "sentAt":        "2026-04-29T14:23:25.873726600Z"
}
```

**Consumer Logs (3 different threads processing in parallel):**
```
[PRODUCER] SENT topic=topic-audit partition=1 offset=8  key=audit-2
[PRODUCER] SENT topic=topic-audit partition=0 offset=2  key=audit-0
[PRODUCER] SENT topic=topic-audit partition=3 offset=2  key=audit-3
[PRODUCER] SENT topic=topic-audit partition=4 offset=6  key=audit-1
[PRODUCER] SENT topic=topic-audit partition=4 offset=7  key=audit-4
[PRODUCER] SENT topic=topic-audit partition=1 offset=9  key=audit-5
[PRODUCER] SENT topic=topic-audit partition=1 offset=10 key=audit-6
[PRODUCER] SENT topic=topic-audit partition=2 offset=2  key=audit-9
[PRODUCER] SENT topic=topic-audit partition=5 offset=4  key=audit-7

[AUDIT-GROUP] thread=...#0-0-C-1 partition=1 offset=8  | 🗄 Persisting: PAYMENT_PROCESSED: txn-555
[AUDIT-GROUP] thread=...#0-1-C-1 partition=3 offset=2  | 🗄 Persisting: USER_LOGOUT: bob
[AUDIT-GROUP] thread=...#0-2-C-1 partition=4 offset=6  | 🗄 Persisting: ORDER_PLACED: #100
[AUDIT-GROUP] thread=...#0-0-C-1 partition=0 offset=2  | 🗄 Persisting: USER_LOGIN: alice
[AUDIT-GROUP] thread=...#0-2-C-1 partition=4 offset=7  | 🗄 Persisting: ITEM_VIEWED: SKU-42
[AUDIT-GROUP] thread=...#0-1-C-1 partition=2 offset=2  | 🗄 Persisting: USER_SIGNUP: eve
[AUDIT-GROUP] thread=...#0-0-C-1 partition=1 offset=9  | 🗄 Persisting: CART_UPDATED: carol
[AUDIT-GROUP] thread=...#0-2-C-1 partition=5 offset=4  | 🗄 Persisting: ORDER_SHIPPED: #98
[AUDIT-GROUP] thread=...#0-0-C-1 partition=1 offset=10 | 🗄 Persisting: USER_LOGIN: dave
[AUDIT-GROUP] thread=...#0-2-C-1 partition=5 offset=5  | 🗄 Persisting: LOGIN_FAILED: frank
[AUDIT-GROUP] thread=...#0-0-C-1 partition=1 offset=11 | 🗄 Persisting: REFUND_ISSUED: txn-301
[AUDIT-GROUP] thread=...#0-2-C-1 partition=4 offset=8  | 🗄 Persisting: PROFILE_UPDATED: grace
```

> Key observation: **3 different thread suffixes** (`#0-0-C-1`, `#0-1-C-1`, `#0-2-C-1`) processing different partitions simultaneously — that is the competing consumer pattern in action.

**Consumer Pattern:**
```
topic-audit (6 partitions: P0–P5)
      │
      └── audit-group (concurrency=3)
              ├── Thread #0-0-C-1  ← P0, P1
              ├── Thread #0-1-C-1  ← P2, P3
              └── Thread #0-2-C-1  ← P4, P5
```

---

### Scenario 5 — Batch Consumer (`topic-payment`)

Consumer polls and receives multiple messages per `poll()` call, processes them as a list.

**Input (bulk — 8 messages):**
```powershell
$body = '{"keyPrefix":"pay","messages":["txn-001: $150.00 Amazon","txn-002: $89.99 Netflix",
  "txn-003: $220.00 Apple","txn-004: $45.50 Spotify","txn-005: $300.00 Microsoft",
  "txn-006: $12.99 Adobe","txn-007: $79.00 GitHub","txn-008: $35.00 JetBrains"]}'

Invoke-RestMethod "http://localhost:8096/api/kafka/payments/bulk" `
  -Method POST -ContentType "application/json" -Body $body | ConvertTo-Json
```

**API Response:**
```json
{
  "status":        "SENT",
  "topic":         "topic-payment",
  "messageCount":  8,
  "scenario":      "Batch Consumer — observe batch of N in [PAYMENT-BATCH] logs",
  "consumerGroups": ["payment-batch-group"],
  "sentAt":        "2026-04-29T14:23:41.217710700Z"
}
```

**Consumer Logs (8 messages delivered as 2 batches):**
```
[PRODUCER] SENT topic=topic-payment partition=0 offset=6  key=pay-0
[PRODUCER] SENT topic=topic-payment partition=0 offset=7  key=pay-3
[PRODUCER] SENT topic=topic-payment partition=0 offset=8  key=pay-4
[PRODUCER] SENT topic=topic-payment partition=0 offset=9  key=pay-5
[PRODUCER] SENT topic=topic-payment partition=1 offset=1  key=pay-2
[PRODUCER] SENT topic=topic-payment partition=2 offset=1  key=pay-1
[PRODUCER] SENT topic=topic-payment partition=0 offset=10 key=pay-6
[PRODUCER] SENT topic=topic-payment partition=0 offset=11 key=pay-7

[PAYMENT-BATCH] ▶ Received batch of 6 payment(s)
[PAYMENT-BATCH]   [1/6] partition=0 offset=6  | 💳 Processing: txn-001: $150.00 Amazon
[PAYMENT-BATCH]   [2/6] partition=0 offset=7  | 💳 Processing: txn-004: $45.50 Spotify
[PAYMENT-BATCH]   [3/6] partition=0 offset=8  | 💳 Processing: txn-005: $300.00 Microsoft
[PAYMENT-BATCH]   [4/6] partition=0 offset=9  | 💳 Processing: txn-006: $12.99 Adobe
[PAYMENT-BATCH]   [5/6] partition=1 offset=1  | 💳 Processing: txn-003: $220.00 Apple
[PAYMENT-BATCH]   [6/6] partition=2 offset=1  | 💳 Processing: txn-002: $89.99 Netflix
[PAYMENT-BATCH] ✔ Batch of 6 payment(s) committed to DB.

[PAYMENT-BATCH] ▶ Received batch of 2 payment(s)
[PAYMENT-BATCH]   [1/2] partition=0 offset=10 | 💳 Processing: txn-007: $79.00 GitHub
[PAYMENT-BATCH]   [2/2] partition=0 offset=11 | 💳 Processing: txn-008: $35.00 JetBrains
[PAYMENT-BATCH] ✔ Batch of 2 payment(s) committed to DB.
```

> Key observation: 8 messages were batched into **two poll() calls** — batch of 6 (from first poll) + batch of 2 (from second poll). The consumer receives `List<String>` instead of individual messages.

**Consumer Pattern:**
```
topic-payment (3 partitions)
      │
      └── payment-batch-group
              └── Thread-1  ← poll() → List<String> messages → process all at once
```

---

### All-in-one Demo Script

```powershell
Write-Host "=== KafkaSpringBootPoc Demo ===" -ForegroundColor Cyan

$base = "http://localhost:8096/api/kafka"
function Send-Kafka($path, $body) {
    $r = Invoke-RestMethod "$base/$path" -Method POST -ContentType "application/json" -Body $body -TimeoutSec 15
    Write-Host "  status=$($r.status) topic=$($r.topic) count=$($r.messageCount)" -ForegroundColor Green
}

Write-Host "`n[1] Single Subscriber - Orders" -ForegroundColor Yellow
Send-Kafka "orders/send" '{"key":"ord-001","message":"Order #001: 2x Laptop, customer: Alice"}'

Write-Host "`n[2] Broadcast - Notifications" -ForegroundColor Yellow
Send-Kafka "notifications/send" '{"key":"notif-001","message":"User registered: alice@example.com"}'

Write-Host "`n[3] CQRS - Inventory" -ForegroundColor Yellow
Send-Kafka "inventory/send" '{"key":"inv-001","message":"SKU-789: stock -5 units, warehouse: London"}'

Write-Host "`n[4] Competing Consumers - Audit Bulk (12 messages)" -ForegroundColor Yellow
Send-Kafka "audit/bulk" '{"keyPrefix":"audit","messages":["USER_LOGIN: alice","ORDER_PLACED: #100","PAYMENT_PROCESSED: txn-555","USER_LOGOUT: bob","ITEM_VIEWED: SKU-42","CART_UPDATED: carol","USER_LOGIN: dave","ORDER_SHIPPED: #98","REFUND_ISSUED: txn-301","USER_SIGNUP: eve","LOGIN_FAILED: frank","PROFILE_UPDATED: grace"]}'

Write-Host "`n[5] Batch Consumer - Payment Bulk (8 messages)" -ForegroundColor Yellow
Send-Kafka "payments/bulk" '{"keyPrefix":"pay","messages":["txn-001: $150.00 Amazon","txn-002: $89.99 Netflix","txn-003: $220.00 Apple","txn-004: $45.50 Spotify","txn-005: $300.00 Microsoft","txn-006: $12.99 Adobe","txn-007: $79.00 GitHub","txn-008: $35.00 JetBrains"]}'

Write-Host "`nDone! Check app logs and http://localhost:8090 (Kafka UI)" -ForegroundColor Green
```

Save as `run-demo.ps1` in the project root and run:
```powershell
.\run-demo.ps1
```

**Demo script output:**
```
=== KafkaSpringBootPoc Demo ===

[1] Single Subscriber - Orders
  status=SENT topic=topic-orders count=1

[2] Broadcast - Notifications
  status=SENT topic=topic-notifications count=1

[3] CQRS - Inventory
  status=SENT topic=topic-inventory count=1

[4] Competing Consumers - Audit Bulk (12 messages)
  status=SENT topic=topic-audit count=12

[5] Batch Consumer - Payment Bulk (8 messages)
  status=SENT topic=topic-payment count=8

Done! Check app logs and http://localhost:8090 (Kafka UI)
```

---

## Kafka UI Dashboard

```
http://localhost:8090
```

Features available in the UI:
- Browse all topics and their partition/offset details
- View consumer groups and lag
- Produce test messages manually
- See message content and headers
- Monitor broker metrics

---

## Consumer Pattern Details

### Pattern 1 — Single Subscriber
```
topic-orders (3 partitions)
       │
       └── order-processor-group
               └── Thread-1  (handles all 3 partitions)
```
Every message processed exactly once, in-order within partition.

---

### Pattern 2 — Multiple Subscribers (Broadcast)
```
topic-notifications (3 partitions)
       │
       ├── mobile-notification-group  ←── ALL messages (offset: 0→N)
       ├── email-notification-group   ←── ALL messages (offset: 0→N)
       └── sms-notification-group     ←── ALL messages (offset: 0→N)
```
Each group has its own offset pointer → receives every message independently.

---

### Pattern 3 — Multiple Groups (CQRS)
```
topic-inventory (6 partitions)
       │
       ├── warehouse-group   ←── ALL events (writes to WMS)
       └── reporting-group   ←── ALL events (writes to analytics DB)
```
Decouples command and query models via event streaming.

---

### Pattern 4 — Competing Consumers
```
topic-audit (6 partitions: P0–P5)
       │
       └── audit-group (concurrency=3)
               ├── Thread-1  ← P0, P1
               ├── Thread-2  ← P2, P3
               └── Thread-3  ← P4, P5
```
Messages load-balanced across threads. No message processed twice.

---

### Pattern 5 — Batch Consumer
```
topic-payment (3 partitions)
       │
       └── payment-batch-group
               └── Thread-1  ← poll() returns List<Message> → process all at once
```

---

## Project Structure

```
KafkaSpringBootPoc/
├── docker-compose.yml                             # ZooKeeper + Kafka + Kafka UI
├── pom.xml
├── README.md
├── run-demo.ps1                                   # All-in-one demo script (optional)
└── src/
    └── main/
        ├── java/com/kafkapoc/
        │   ├── KafkaSpringBootPocApplication.java # Spring Boot entry point
        │   ├── config/
        │   │   ├── KafkaConfig.java               # ProducerFactory, ConsumerFactory, batch factory
        │   │   ├── KafkaTopicConfig.java          # Topic declarations (auto-created on startup)
        │   │   └── OpenApiConfig.java             # Swagger/OpenAPI metadata
        │   ├── controller/
        │   │   └── KafkaController.java           # REST endpoints for all 5 scenarios
        │   ├── producer/
        │   │   └── KafkaMessageProducer.java      # KafkaTemplate wrapper
        │   ├── consumer/
        │   │   ├── OrderConsumer.java             # Scenario 1: Single Subscriber
        │   │   ├── NotificationConsumer.java      # Scenario 2: Multiple Subscribers (Broadcast)
        │   │   ├── InventoryConsumer.java         # Scenario 3: Multiple Groups (CQRS)
        │   │   ├── AuditLogConsumer.java          # Scenario 4: Competing Consumers
        │   │   └── PaymentConsumer.java           # Scenario 5: Batch Consumer
        │   └── model/
        │       ├── MessageRequest.java
        │       ├── BulkMessageRequest.java
        │       └── MessageResponse.java
        └── resources/
            └── application.yml                    # Server port 8096, topic names, logging
```

---

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `Connection refused localhost:9092` | Kafka not started | Run `docker compose up -d` first |
| Topics not created on startup | Kafka not ready | Wait 15s for Kafka health check, restart app |
| `docker compose` not found | Docker Desktop < v2 | Install Docker Desktop latest |
| App fails with `UnknownHostException` | Kafka container not healthy | Run `docker compose ps` and check status |
| Consumer not receiving messages | Wrong bootstrap-servers | Ensure `application.yml` has `localhost:9092` |
| Port 8096 already in use | Another app running | `netstat -ano \| findstr :8096`, kill PID |
| Port 9092 already in use | Local Kafka running | Stop local Kafka or change port in `docker-compose.yml` |

### Verify Kafka is running
```powershell
# Check containers
docker compose ps

# List topics (should show all 5 after app starts)
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# Check consumer groups
docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --list
```

### View consumer group offsets
```powershell
docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 `
    --describe --group order-processor-group
```
