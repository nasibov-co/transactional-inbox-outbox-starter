# Transactional Inbox/Outbox Core

Database-independent contracts and the event-processing pipeline for the transactional inbox/outbox pattern.

It provides `Event`, `BaseEvent`, `EventHandler`, `EventRepository`, polling, worker orchestration, retry configuration, lifecycle management, and optional Micrometer metrics. The core module does not contain any database access implementation.

Use this module when implementing a custom `EventRepository` adapter. Enable processing with `transactional.enabled=true`; provide an `EventRepository` bean for the processor to start.

```kotlin
dependencies {
    implementation("io.github.fnasibov:transactional-inbox-outbox-core:3.0.0")
}
```

For R2DBC applications, prefer `transactional-inbox-outbox-starter-r2dbc`, which includes this module and the default R2DBC repository.
