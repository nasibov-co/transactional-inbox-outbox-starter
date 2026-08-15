# Transactional Inbox/Outbox R2DBC Adapter

R2DBC implementation for `transactional-inbox-outbox-core`.

This module supplies `R2dbcEventRepository`, which uses `R2dbcEntityTemplate`, `ReactiveTransactionManager`, `FOR UPDATE SKIP LOCKED`, and the core retry settings to persist event lifecycle changes. Conditional bean creation lives in the persistence-neutral `transactional-inbox-outbox-autoconfigure` module.

The module depends on `transactional-inbox-outbox-core`. Applications should normally use `transactional-inbox-outbox-starter-r2dbc` instead of depending on this adapter directly.
