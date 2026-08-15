# Transactional Inbox/Outbox JDBC Demo

Blocking Spring Boot MVC demo for `transactional-inbox-outbox-starter-jdbc`.

The demo uses PostgreSQL over JDBC, creates one `PENDING` event from `data.sql`, and handles it with the non-suspending `JdbcDemoEventHandler`. It also registers a custom `BlockingFetchBatchStrategy` that polls events by highest priority first.

Start PostgreSQL from this directory:

```shell
docker compose up -d
```

Then run the app from the repository root on Windows:

```powershell
.\gradlew.bat :transactional-inbox-outbox-demo-jdbc:bootRun
```

Or on macOS/Linux:

```shell
./gradlew :transactional-inbox-outbox-demo-jdbc:bootRun
```

The JDBC demo runs on port `8081`. Create a new pending event:

```shell
curl -X POST http://localhost:8081/demo-events \
  -H "Content-Type: application/json" \
  -d '{"payload":"Hello from JDBC HTTP","priority":25}'
```
