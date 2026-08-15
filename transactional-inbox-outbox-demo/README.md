# Transactional Inbox/Outbox Demo

Spring Boot R2DBC demo module for `transactional-inbox-outbox-starter-r2dbc`.

The demo uses PostgreSQL over R2DBC, creates one `PENDING` event from `data.sql`, and handles it with `DemoEventHandler`.

The demo consumes the starter from the local Gradle project, but it is not configured for publishing.

Start PostgreSQL from this directory:

```shell
docker compose up -d
```

Then run the app on Windows:

```powershell
..\gradlew.bat :transactional-inbox-outbox-demo:bootRun
```

Or run the app on macOS/Linux:

```shell
../gradlew :transactional-inbox-outbox-demo:bootRun
```

From the repository root on Windows:

```powershell
.\gradlew.bat :transactional-inbox-outbox-demo:bootRun
```

From the repository root on macOS/Linux:

```shell
./gradlew :transactional-inbox-outbox-demo:bootRun
```

Create a new pending event:

```shell
curl -X POST http://localhost:8080/demo-events \
  -H "Content-Type: application/json" \
  -d '{"payload":"Hello from HTTP","priority":25}'
```

The demo also registers `PriorityDemoEventFetchBatchStrategy`, a custom `FetchBatchStrategy` that fetches `DemoEvent` rows by highest `priority` first.
