# Transactional Inbox/Outbox Core

Database-independent contracts and the event-processing pipeline for the transactional inbox/outbox pattern.

It provides `Event`, `BaseEvent`, `EventHandler`, `EventRepository`, polling, worker orchestration, retry configuration, lifecycle management, and optional Micrometer metrics. The core module does not contain any database access implementation.

Use this module when implementing a custom `EventRepository` adapter or wiring the processor manually. Spring bean registration and `transactional.enabled` handling are provided by the separate `transactional-inbox-outbox-autoconfigure` module.

```kotlin
dependencies {
    implementation("io.github.fnasibov:transactional-inbox-outbox-core:4.0.0")
}
```

Applications should normally use either `transactional-inbox-outbox-starter-r2dbc` or `transactional-inbox-outbox-starter-jdbc`, which includes this module and the matching default repository.
