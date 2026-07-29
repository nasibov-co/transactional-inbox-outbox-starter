# Transactional Inbox/Outbox R2DBC Adapter

R2DBC implementation for `transactional-inbox-outbox-core`.

This module supplies `BaseEventRepository`, which uses `R2dbcEntityTemplate`, `ReactiveTransactionManager`, `FOR UPDATE SKIP LOCKED`, and the core retry settings to persist event lifecycle changes. Its auto-configuration creates the repository only when R2DBC infrastructure is present and `transactional.enabled=true`.

The module depends on `transactional-inbox-outbox-core`. Applications should normally use `transactional-inbox-outbox-starter-r2dbc` instead of depending on this adapter directly.
