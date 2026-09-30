# AGENTS.md

Contributor/agent guide for this repository. User-facing documentation lives in `README.md`; this file is about
how to change the code safely. Do not duplicate the README here.

## What this is

A Kotlin/Spring Boot multi-module starter implementing the transactional inbox/outbox pattern for both JDBC and
R2DBC. Core processing uses Kotlin coroutines. The engine polls event rows, dispatches them to typed handlers,
applies retry policy, and moves exhausted events to `DEAD_LETTER`.

## Module map

| Module | Contents |
| --- | --- |
| `transactional-inbox-outbox-core` | Database-independent contracts, processing pipeline, retry, config model |
| `transactional-inbox-outbox-jdbc` | JDBC repository adapter |
| `transactional-inbox-outbox-r2dbc` | R2DBC repository adapter |
| `transactional-inbox-outbox-autoconfigure` | Shared conditional auto-configuration |
| `transactional-inbox-outbox-starter-jdbc` | JDBC dependency starter |
| `transactional-inbox-outbox-starter-r2dbc` | R2DBC dependency starter |
| `transactional-inbox-outbox-demo` | R2DBC demo app (PostgreSQL, port 8080) |
| `transactional-inbox-outbox-demo-jdbc` | JDBC demo app (PostgreSQL, port 8081 / DB port 5434) |

Tests use mocks/in-memory stores and do not require a running database. The demo apps do require PostgreSQL, started
via their module `docker-compose.yml` (see README "Demos").

## Build and test

Use the Gradle wrapper from the repo root. On Windows: `.\gradlew.bat ...`; on macOS/Linux: `./gradlew ...`.

```bash
./gradlew build                         # full build + all tests
./gradlew test                          # all tests
./gradlew :transactional-inbox-outbox-jdbc:test
./gradlew :transactional-inbox-outbox-r2dbc:test
```

Prefer the narrowest module test during iteration; run the full build before finishing a cross-cutting change.

## Working style

- **TDD**: write or extend the failing test first, then implement. Keep tests focused on observable behavior.
- **English**: all code comments, KDoc, and test names are in English. Match the existing style (`backtick` test
  method names, `kotlin.test` assertions).
- **Comments**: explain non-obvious intent only; do not narrate the code.
- **Docs**: README and CONTRIBUTING are user-facing. Do not add or update docs unless the task asks for it.
- **Backward compatibility**: this library is consumed by other applications. Preserve public APIs, configuration
  property names, and existing runtime behavior by default. Call out intentional breaking changes explicitly.
- **Public surface**: API types live under `...core.api` and `...core.api.model`. Treat changes there as public.

## JDBC / R2DBC symmetry

JDBC and R2DBC adapters must stay behaviorally aligned for shared features: fetching/locking, status transitions,
batch-key grouping, and retry handoff. When you change one adapter, check the sibling and its tests. The
user-visible `@BatchKey` fetch contract is covered by parallel regression suites:

- `transactional-inbox-outbox-jdbc/src/test/.../JdbcBatchKeyFetchTest.kt`
- `transactional-inbox-outbox-r2dbc/src/test/.../R2dbcBatchKeyFetchTest.kt`

Keep these two suites equivalent in intent.

## Batch semantics and limitations (current behavior)

- `@BatchKey` marks exactly one persisted property. The default fetch path selects one key group per fetch, ordered
  by creation time, capped by `transactional.polling.batch-size`; a large group drains over several fetches.
- `BatchEventHandler.handleBatch` receives the whole fetched batch and must return a `BatchResult` containing exactly
  one outcome for every event id in the batch. Missing or extra ids reject the whole batch as failed; a thrown
  exception retries every event in the batch.
- `BatchEventOutcome.PROCESSED` records the event; `RETRY` routes it through the normal retry lifecycle (FAILED with
  backoff, then DEAD_LETTER when attempts are exhausted).
- An event type must be registered with either `EventHandler` beans or `BatchEventHandler` beans, never both.
- A custom `FetchBatchStrategy<E>` replaces the default fetch path and bypasses starter `@BatchKey` grouping.
- Models without `@BatchKey` do **not** get homogeneous batches; the default path may return mixed keys.
- There is no active heartbeat. A batch's claim is derived from `PROCESSING` rows whose `last_attempt_at` is newer
  than `transactional.polling.processing-stale-timeout`. A handler that runs longer than that timeout can have its
  batch treated as stale and reclaimed by another fetch. Keep this in mind when touching fetch/claim logic.

## Known baseline issue

`transactional-inbox-outbox-autoconfigure/src/test/.../TransactionalInboxOutboxAutoconfigurationTest.kt`
("auto configuration imports define explicit loading order") currently fails on a clean base: it asserts the imports
list omits `TransactionalInboxOutboxJdbcConversionsAutoConfiguration`, but
`META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports` includes it (and places it between
the R2DBC and JDBC auto-configurations). Do not treat the test's expected list as a permanent invariant. Recheck
whether this is still failing before attributing a test failure to your change; if you touch import order, update
the test and the imports file together.

## Before finishing

1. `git status` / `git diff` — confirm only intended files changed and no unrelated or generated files are included.
2. Run the narrowest relevant test; state honestly what you ran and what you did not.
3. Never commit unrelated changes, and never push.