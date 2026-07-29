# Transactional Inbox/Outbox R2DBC Starter

Spring Boot starter for the R2DBC transactional inbox/outbox implementation.

It brings the database-independent `transactional-inbox-outbox-core`, the R2DBC repository adapter, Spring Data R2DBC, and validation support.

```kotlin
dependencies {
    implementation("io.github.fnasibov:transactional-inbox-outbox-starter-r2dbc:3.0.0")
}
```

Configure an R2DBC connection and `ReactiveTransactionManager`, then set `transactional.enabled=true`. See the repository root `README.md` for event, handler, schema, and property examples.
