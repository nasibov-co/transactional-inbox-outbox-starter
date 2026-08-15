[![Maven Central](https://img.shields.io/maven-central/v/io.github.fnasibov/transactional-inbox-outbox-starter-r2dbc?label=maven%20central)](https://central.sonatype.com/artifact/io.github.fnasibov/transactional-inbox-outbox-starter-r2dbc)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
<img referrerpolicy="no-referrer-when-downgrade" src="https://static.scarf.sh/a.png?x-pxid=dfbc9d9c-c0c5-4e8d-b14c-c48fe4d77ee9" />
# Transactional Inbox/Outbox Starter

A lightweight Spring Boot starter for transactional inbox/outbox processing with either **JDBC** or **R2DBC**. The processing engine uses Kotlin coroutines internally, while client code can choose suspending or ordinary blocking contracts.

The starter polls event rows from your database, dispatches them to typed handlers, applies retry rules, and moves exhausted events to `DEAD_LETTER`.

## Installation

Choose exactly one starter.

R2DBC:

```kotlin
dependencies {
    implementation("io.github.fnasibov:transactional-inbox-outbox-starter-r2dbc:4.0.0")
}
```

JDBC:

```kotlin
dependencies {
    implementation("io.github.fnasibov:transactional-inbox-outbox-starter-jdbc:4.0.0")
}
```

Auto-configuration selects the repository adapter from the infrastructure supplied by the chosen starter: `R2dbcEntityTemplate` plus `ReactiveTransactionManager`, or `JdbcAggregateOperations` plus `PlatformTransactionManager`. A client-provided `EventRepository` overrides both defaults.

## Project Modules

This repository follows the Spring Boot starter layout:

| Module | Purpose |
| --- | --- |
| `transactional-inbox-outbox-core` | Database-independent contracts, processing pipeline, retry policy, and configuration model. |
| `transactional-inbox-outbox-r2dbc` | R2DBC repository adapter. |
| `transactional-inbox-outbox-jdbc` | JDBC repository adapter. |
| `transactional-inbox-outbox-autoconfigure` | Shared conditional auto-configuration that resolves the available adapter. |
| `transactional-inbox-outbox-starter-r2dbc` | R2DBC dependency starter. |
| `transactional-inbox-outbox-starter-jdbc` | JDBC dependency starter. |
| `transactional-inbox-outbox-demo` | Reactive R2DBC demo application. |
| `transactional-inbox-outbox-demo-jdbc` | Blocking JDBC demo application. |

## Quick Start

### 1. Define an event entity

Each event type is a Spring Data Relational entity. Extend `BaseEvent` to inherit the lifecycle columns and add only your domain-specific fields. The same model contract works with Spring Data JDBC and R2DBC.

```kotlin
import com.fnasibov.transactional.inbox.outbox.core.api.model.BaseEvent
import org.springframework.data.relational.core.mapping.Table
import java.util.UUID

@Table("payment_events")
data class PaymentEvent(
    val paymentId: UUID,
    val amount: Long,
    val currency: String
) : BaseEvent()
```

`BaseEvent` provides `id`, `status`, `createdAt`, `updatedAt`, `retryCount`, `lastAttemptAt`, and `nextRetryAt` with sensible defaults. If your model cannot extend it, implement `Event` directly and provide the same lifecycle fields yourself.

The table name is read from `@Table`, so the annotation is required for default polling.

### 2. Create the table

The default repository expects the lifecycle columns from `BaseEvent` / `Event` to exist in snake case.

```sql
CREATE TABLE payment_events (
    id UUID PRIMARY KEY,
    status VARCHAR(32) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ,
    retry_count INT NOT NULL DEFAULT 0,
    last_attempt_at TIMESTAMPTZ,
    next_retry_at TIMESTAMPTZ,

    payment_id UUID NOT NULL,
    amount BIGINT NOT NULL,
    currency VARCHAR(3) NOT NULL
);

CREATE INDEX idx_payment_events_polling
    ON payment_events (status, next_retry_at, created_at);
```

### 3. Register a handler

Every event type that should be processed must have at least one `EventHandler` bean.

```kotlin
import com.fnasibov.transactional.inbox.outbox.core.api.EventHandler
import org.springframework.stereotype.Component

@Component
class PaymentEventHandler(
    private val paymentPublisher: PaymentPublisher
) : EventHandler<PaymentEvent> {

    override fun supportedEventType(): Class<PaymentEvent> =
        PaymentEvent::class.java

    override suspend fun handle(event: PaymentEvent) {
        paymentPublisher.publish(
            paymentId = event.paymentId,
            amount = event.amount,
            currency = event.currency
        )
    }

    override suspend fun handleDeadLetter(event: PaymentEvent, error: Throwable) {
        // Called only when the event is moved to DEAD_LETTER.
        // Use it for logging, metrics, alerts, or compensating actions.
    }
}
```

Multiple handlers can support the same event type. They are executed sequentially for a consumed event; if any handler fails, the event is marked as failed according to the retry policy.

Clients that do not use coroutines can implement `BlockingEventHandler` instead:

```kotlin
@Component
class PaymentEventHandler : BlockingEventHandler<PaymentEvent> {
    override fun supportedEventType() = PaymentEvent::class.java

    override fun handle(event: PaymentEvent) {
        paymentPublisher.publish(event)
    }

    override fun handleDeadLetter(event: PaymentEvent, error: Throwable) {
        logger.error("Payment event moved to dead letter", error)
    }
}
```

Blocking handlers are moved to `Dispatchers.IO` before execution, so they do not block the processor's default coroutine dispatcher.

The same option exists for infrastructure overrides: implement `BlockingEventRepository` when a custom repository is required without coroutine methods. It is automatically adapted and takes precedence over both default adapters.

### 4. Enable the starter

```yaml
transactional:
  enabled: true
```

When enabled, auto-configuration creates the repository, processor, coroutine scope, and lifecycle starter. Processing starts with the Spring application.

## Demos

Two standalone Spring Boot demo applications are available: the reactive R2DBC demo in `transactional-inbox-outbox-demo` and the blocking JDBC demo in `transactional-inbox-outbox-demo-jdbc`.

### R2DBC

Start PostgreSQL:

```shell
docker compose -f transactional-inbox-outbox-demo/docker-compose.yml up -d
```

Run it from the repository root on Windows:

```powershell
.\gradlew.bat :transactional-inbox-outbox-demo:bootRun
```

Run it from the repository root on macOS/Linux:

```shell
./gradlew :transactional-inbox-outbox-demo:bootRun
```

Create a new pending demo event:

```shell
curl -X POST http://localhost:8080/demo-events \
  -H "Content-Type: application/json" \
  -d '{"payload":"Hello from HTTP","priority":25}'
```

### JDBC

Start its PostgreSQL instance on port `5434`:

```shell
docker compose -f transactional-inbox-outbox-demo-jdbc/docker-compose.yml up -d
```

Run the blocking demo on port `8081`:

```powershell
.\gradlew.bat :transactional-inbox-outbox-demo-jdbc:bootRun
```

Create a new pending JDBC event:

```shell
curl -X POST http://localhost:8081/demo-events \
  -H "Content-Type: application/json" \
  -d '{"payload":"Hello from JDBC HTTP","priority":25}'
```

## Configuration

All properties live under the `transactional` prefix.

```yaml
transactional:
  enabled: true

  polling:
    # Polling interval while events are available.
    active-interval: 100ms

    # Maximum idle interval after exponential backoff when no events are found.
    max-idle-interval: 30s

    # Number of rows fetched by one poller in one database batch.
    batch-size: 15

    # Internal channel capacity between pollers and workers.
    channel-capacity: 25

    # Time after which PROCESSING events are considered stale and eligible again.
    processing-stale-timeout: 5m

  processing:
    # Default number of worker coroutines per event type.
    concurrency: 5

    # Optional event type specific processing settings.
    event-types:
      - event-type: PaymentEvent
        concurrency: 2
        retry:
          max-attempts: 10
          initial-delay: 5s
          multiplier: 1.5
          max-delay: 10m
      - event-type: com.example.billing.InvoiceEvent
        concurrency: 8
        retry:
          max-attempts: 5

    # Maximum time to drain already fetched events during shutdown.
    shutdown-timeout: 30s

  retry:
    # Number of failed processing attempts before DEAD_LETTER.
    max-attempts: 3

    # Exponential backoff settings before a FAILED event is retried.
    initial-delay: 1s
    multiplier: 2.0
    max-delay: 1m
```

Defaults:

| Property | Default |
| --- | --- |
| `transactional.enabled` | `false` |
| `transactional.polling.active-interval` | `100ms` |
| `transactional.polling.max-idle-interval` | `30s` |
| `transactional.polling.batch-size` | `15` |
| `transactional.polling.channel-capacity` | `25` |
| `transactional.polling.processing-stale-timeout` | `5m` |
| `transactional.processing.concurrency` | `5` |
| `transactional.processing.event-types` | empty |
| `transactional.processing.shutdown-timeout` | `30s` |
| `transactional.retry.max-attempts` | `3` |
| `transactional.retry.initial-delay` | `1s` |
| `transactional.retry.multiplier` | `2.0` |
| `transactional.retry.max-delay` | `1m` |

Event-specific retry fields under `transactional.processing.event-types[].retry` are optional. Any omitted field falls back to the matching global `transactional.retry.*` value.

Duration properties support readable values such as `100ms`, `1s`, `30s`, and `1m`.

## Custom Batch Fetching

By default, the starter fetches eligible events from the table mapped by `@Table`, locks them with `FOR UPDATE SKIP LOCKED`, updates their status to `PROCESSING`, and returns the selected entities.

Register `FetchBatchStrategy` when an event type needs custom selection, priority ordering, partitioning, or database-specific locking.

```kotlin
import com.fnasibov.transactional.inbox.outbox.core.api.FetchBatchStrategy
import org.springframework.stereotype.Component

@Component
class PaymentFetchStrategy(
    private val repository: CustomPaymentEventRepository
) : FetchBatchStrategy<PaymentEvent> {

    override val eventType: Class<PaymentEvent> =
        PaymentEvent::class.java

    override suspend fun fetchBatch(): List<PaymentEvent> {
        return repository.fetchPriorityBatch()
    }
}
```

Custom strategies are responsible for their own locking, transaction boundaries, and status transitions.

For a non-suspending implementation use `BlockingFetchBatchStrategy<E>` with an ordinary `fun fetchBatch(): List<E>`. It is adapted to the same processing pipeline and executed on `Dispatchers.IO`.

## Processing Flow

```text
Database -> EventPoller -> Event type channel -> EventWorker -> EventHandler(s)
```

For each registered event type, the starter starts a poller, a dedicated coroutine channel, and worker coroutines for that type. If an item in `transactional.processing.event-types` matches the event class by fully qualified or simple name, its `concurrency` controls that type's worker count; otherwise the starter falls back to `transactional.processing.concurrency`.

Lifecycle statuses:

```text
PENDING -> PROCESSING -> PROCESSED
                    \-> FAILED -> PROCESSING
                    \-> DEAD_LETTER
```

Failure behavior:

- Handler failures call `markAsFailed`.
- Failed events are retried with exponential backoff using `retry.initial-delay`, `retry.multiplier`, and `retry.max-delay`.
- Retry settings can be overridden per event type using `transactional.processing.event-types[].retry`.
- Once `retry.max-attempts` is reached, the event moves to `DEAD_LETTER`.
- `handleDeadLetter` is called only after the event reaches `DEAD_LETTER`.

## Observability

When a `MeterRegistry` is available, the starter publishes Micrometer meters:

| Meter | Meaning |
| --- | --- |
| `transactional.events.fetched` | Number of events fetched for processing |
| `transactional.events.processed` | Number of successfully processed events |
| `transactional.events.failed` | Number of processing failures |
| `transactional.events.dead_letter` | Number of events moved to `DEAD_LETTER` |
| `transactional.events.processing.duration` | Handler processing duration |

When Spring Boot health contributor support is on the classpath, the starter also contributes a `transactionalInboxOutboxHealthIndicator` bean.

## Notes

- Both starters are database-backed and do not require an external broker.
- Add one of `starter-jdbc` or `starter-r2dbc`; the shared auto-configuration backs off when the required infrastructure beans are absent or when the client supplies `EventRepository`.
- Default polling uses `FOR UPDATE SKIP LOCKED`, so it is intended for databases that support this locking style.
- A handler is required for an event type to be polled because pollers are created from registered handler event types.
- Extend `BaseEvent` for the standard lifecycle columns, or implement `Event` directly when you need a fully custom model.
- Event classes can contain any domain-specific columns in addition to the lifecycle fields required by the starter.

## Contact

Feel free to contact me about anything at fakhri.nasibov@gmail.com.
