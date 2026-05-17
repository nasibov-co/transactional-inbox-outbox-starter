# Transactional Inbox/Outbox R2DBC Starter

A lightweight Spring Boot starter for implementing the **Transactional Outbox / Inbox pattern** using **R2DBC + Coroutines**.

It provides a ready-to-use event processing pipeline with database-backed reliability, retries and concurrent processing.

---

# 🚀 Quick Start (How to use)

This section is everything you need to start using the starter.

## 1. Define an event

All events must implement `Event` and map to a DB table:

```kotlin
@Table("payment_events")
data class PaymentEvent(
    override val id: UUID,
    override val payload: String,
    override val status: EventStatus,
    override val createdAt: ZonedDateTime,
    override val updatedAt: ZonedDateTime?,
    override val retryCount: Int,
    override val lastAttemptAt: ZonedDateTime?
) : Event
```

---

## 2. Create a handler

Each event type must have at least one handler:

```kotlin
@Component
class PaymentEventHandler : EventHandler<PaymentEvent> {

    override fun supportedEventType() = PaymentEvent::class.java

    override suspend fun handle(event: PaymentEvent) {
        println("Processing payment ${event.id}")
    }
}
```

You can add multiple handlers for the same event type — they will all be executed.

---

## 3. Enable configuration

```yaml
transactional:
  enabled: true
```

Optional tuning:

```yaml
transactional:
  processing:
    concurrency: 5

  polling:
    batchSize: 15
    channelCapacity: 25

  retry:
    maxImmediateAttempts: 3
```

---

## 4. Done

That’s it.

The system will:

* poll events from DB
* push them into internal channel
* execute handlers concurrently
* retry failed events automatically
* move unrecoverable events to DEAD_LETTER

---

# ⚙️ What happens internally (high-level)

If you’re curious, the flow is:

```
Database → Poller → Channel → Worker → Handlers
```

But you don’t need to interact with these components directly.

---

# 🔧 Extension points (optional)

## Custom fetch logic per event type

If default polling is not enough:

```kotlin
@Component
class PaymentFetchStrategy : FetchBatchStrategy<PaymentEvent> {

    override val eventType = PaymentEvent::class.java

    override suspend fun fetchBatch(): List<PaymentEvent> {
        return emptyList()
    }
}
```

Use this for:

* priority queues
* partitioned processing
* custom locking logic

---

# 🧠 Architecture (internal details)

This section is only relevant if you need to modify or debug the starter.

## Core components

### EventPoller

Polls database and pushes events into channel with adaptive backoff.

### EventWorker

Consumes events and executes handlers with configurable concurrency.

* handles retry logic
* handles dead-letter transitions
* isolates failures per event

### EventRepository

Responsible for:

* batch fetching (`SKIP LOCKED`)
* updating lifecycle state
* retry management

### EventProcessor

Orchestrates pollers + workers.

### EventProcessorStarter

Spring lifecycle integration (auto start/stop).

---

## Event lifecycle

```
PENDING → PROCESSING → PROCESSED
                 ↘ FAILED → RETRY → DEAD_LETTER
```

---

## Key properties

* DB-backed queue (no external broker required)
* coroutine-based concurrency
* safe polling with `FOR UPDATE SKIP LOCKED`
* horizontal scalability
* per-event customization via handlers and fetch strategies
