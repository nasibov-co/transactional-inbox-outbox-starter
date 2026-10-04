package com.fnasibov.transactional.inbox.outbox.r2dbc

import com.fnasibov.transactional.inbox.outbox.core.api.model.BaseEvent
import com.fnasibov.transactional.inbox.outbox.core.api.model.BatchKey
import com.fnasibov.transactional.inbox.outbox.core.api.model.EventStatus
import com.fnasibov.transactional.inbox.outbox.core.configuration.TransactionalProperties
import io.r2dbc.spi.ConnectionFactories
import io.r2dbc.spi.ConnectionFactory
import io.r2dbc.spi.ConnectionFactoryOptions
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.reactive.awaitSingle
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.autoconfigure.EnableAutoConfiguration
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.data.relational.core.mapping.Table
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.r2dbc.core.bind
import org.springframework.transaction.reactive.TransactionalOperator
import org.springframework.transaction.ReactiveTransactionManager
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import java.time.ZonedDateTime
import java.util.UUID
import kotlin.test.assertEquals

/**
 * PostgreSQL integration tests for [R2dbcEventRepository], executed against a
 * real database through Testcontainers.
 *
 * The suite complements [R2dbcBatchKeyFetchTest], which pins the same scenario
 * intent against a stateful fake: here the actual polling SQL runs on
 * PostgreSQL, proving batch-key selection, durable claim exclusion and release,
 * stale-claim recovery, and retry/dead-letter status transitions on real rows.
 */
@Testcontainers
@SpringBootTest(classes = [R2dbcPostgresTestApp::class])
class R2dbcEventRepositoryPostgresIntegrationTest {

    @Autowired
    private lateinit var template: R2dbcEntityTemplate

    @Autowired
    private lateinit var transactionManager: ReactiveTransactionManager

    private val client: DatabaseClient
        get() = template.databaseClient

    @BeforeEach
    fun resetSchema() {
        execute(CREATE_BATCH_KEY_TABLE)
        execute(CREATE_LEGACY_TABLE)
        execute("DELETE FROM batch_key_events")
        execute("DELETE FROM legacy_events")
    }

    @Test
    fun `annotated fetch claims one key group capped by the batch size`() = runBlocking {
        val a1 = insertBatchEvent("account-a", baseTime)
        val a2 = insertBatchEvent("account-a", baseTime.plusSeconds(1))
        val b1 = insertBatchEvent("account-b", baseTime.plusSeconds(2))
        val a3 = insertBatchEvent("account-a", baseTime.plusSeconds(3))
        val repository = repository(batchSize = 2)

        val batch = repository.fetchBatch(BatchKeyEvent::class.java)

        assertEquals(listOf(a1, a2), batch.map { it.id })
        assertEquals(listOf("account-a"), batch.map { it.accountId }.distinct())
        assertEquals(EventStatus.PROCESSING, statusOf(a1))
        assertEquals(EventStatus.PROCESSING, statusOf(a2))
        assertEquals(EventStatus.PENDING, statusOf(b1))
        assertEquals(EventStatus.PENDING, statusOf(a3))
        assertEquals(2, countRows("last_attempt_at IS NOT NULL"), "claimed rows must record the attempt time")
    }

    @Test
    fun `a claimed key group cannot be claimed again while its batch is processing`() = runBlocking {
        val a1 = insertBatchEvent("account-a", baseTime)
        val a2 = insertBatchEvent("account-a", baseTime.plusSeconds(1))
        val a3 = insertBatchEvent("account-a", baseTime.plusSeconds(2))
        val repository = repository(batchSize = 2)

        val first = repository.fetchBatch(BatchKeyEvent::class.java)
        val second = repository.fetchBatch(BatchKeyEvent::class.java)

        assertEquals(listOf(a1, a2), first.map { it.id })
        assertEquals(
            emptyList(),
            second,
            "the third event must stay unclaimed while the first batch is processing"
        )
        assertEquals(EventStatus.PENDING, statusOf(a3))
    }

    @Test
    fun `different keys stay claimable while another key is being processed`() = runBlocking {
        val a1 = insertBatchEvent("account-a", baseTime)
        val a2 = insertBatchEvent("account-a", baseTime.plusSeconds(1))
        val b1 = insertBatchEvent("account-b", baseTime.plusSeconds(2))
        val repository = repository(batchSize = 1)

        val first = repository.fetchBatch(BatchKeyEvent::class.java)
        assertEquals(listOf(a1), first.map { it.id })
        val second = repository.fetchBatch(BatchKeyEvent::class.java)
        assertEquals(
            listOf(b1),
            second.map { it.id },
            "a different key must remain claimable while account-a is processing"
        )
        assertEquals(emptyList(), repository.fetchBatch(BatchKeyEvent::class.java))
        assertEquals(EventStatus.PENDING, statusOf(a2))

        repository.markAsProcessed(first.single())
        repository.markAsProcessed(second.single())

        assertEquals(
            listOf(a2),
            repository.fetchBatch(BatchKeyEvent::class.java).map { it.id },
            "the account-a claim must be released once its batch completes"
        )
    }

    @Test
    fun `key claim is released only when the last event of the batch completes`() = runBlocking {
        val a1 = insertBatchEvent("account-a", baseTime)
        val a2 = insertBatchEvent("account-a", baseTime.plusSeconds(1))
        val a3 = insertBatchEvent("account-a", baseTime.plusSeconds(2))
        val repository = repository(batchSize = 2)

        val first = repository.fetchBatch(BatchKeyEvent::class.java)
        assertEquals(listOf(a1, a2), first.map { it.id })

        repository.markAsProcessed(first.first())
        assertEquals(
            emptyList(),
            repository.fetchBatch(BatchKeyEvent::class.java),
            "the key stays claimed until the whole batch finished processing"
        )

        repository.markAsProcessed(first.last())
        assertEquals(listOf(a3), repository.fetchBatch(BatchKeyEvent::class.java).map { it.id })
        assertEquals(EventStatus.PROCESSED, statusOf(a1))
        assertEquals(EventStatus.PROCESSED, statusOf(a2))
    }

    @Test
    fun `key claim recovers after the processing stale timeout`() = runBlocking {
        val a1 = insertBatchEvent("account-a", baseTime)
        val a2 = insertBatchEvent("account-a", baseTime.plusSeconds(1))
        val repository = repository(batchSize = 2)

        val first = repository.fetchBatch(BatchKeyEvent::class.java)
        assertEquals(listOf(a1, a2), first.map { it.id })
        assertEquals(emptyList(), repository.fetchBatch(BatchKeyEvent::class.java))

        // Simulate a crashed instance: the claim ages past the stale-processing lease.
        execute("UPDATE batch_key_events SET last_attempt_at = last_attempt_at - INTERVAL '1 day'")

        assertEquals(
            first.map { it.id },
            repository.fetchBatch(BatchKeyEvent::class.java).map { it.id },
            "an abandoned claim must expire with the stale-processing lease"
        )
    }

    @Test
    fun `a failed event retries only after its next retry time`() = runBlocking {
        val id = insertBatchEvent("account-a", baseTime)
        val repository = repository(batchSize = 5)

        val batch = repository.fetchBatch(BatchKeyEvent::class.java)
        assertEquals(listOf(id), batch.map { it.id })

        val nextStatus = repository.markAsFailed(batch.single())
        assertEquals(EventStatus.FAILED, nextStatus)
        assertEquals(EventStatus.FAILED, statusOf(id))
        assertEquals(1, retryCountOf(id))
        assertEquals(1, countRows("next_retry_at IS NOT NULL"), "a retryable failure must schedule the next attempt")

        assertEquals(
            emptyList(),
            repository.fetchBatch(BatchKeyEvent::class.java),
            "a failed event must wait for its next retry time"
        )

        execute("UPDATE batch_key_events SET next_retry_at = next_retry_at - INTERVAL '1 hour'")

        assertEquals(
            listOf(id),
            repository.fetchBatch(BatchKeyEvent::class.java).map { it.id },
            "a due retry must become eligible again"
        )
        assertEquals(EventStatus.PROCESSING, statusOf(id))
    }

    @Test
    fun `an event with exhausted retries moves to dead letter`() = runBlocking {
        val id = insertBatchEvent("account-a", baseTime)
        val repository = repository(batchSize = 5, maxAttempts = 1)

        val batch = repository.fetchBatch(BatchKeyEvent::class.java)
        val nextStatus = repository.markAsFailed(batch.single())

        assertEquals(EventStatus.DEAD_LETTER, nextStatus)
        assertEquals(EventStatus.DEAD_LETTER, statusOf(id))
        assertEquals(1, countRows("next_retry_at IS NULL"), "a dead-lettered event must not schedule another attempt")
        assertEquals(emptyList(), repository.fetchBatch(BatchKeyEvent::class.java))
    }

    @Test
    fun `events with a null batch key form their own group`() = runBlocking {
        val n1 = insertBatchEvent(null, baseTime)
        val n2 = insertBatchEvent(null, baseTime.plusSeconds(1))
        val n3 = insertBatchEvent(null, baseTime.plusSeconds(2))
        val a1 = insertBatchEvent("account-a", baseTime.plusSeconds(3))
        val repository = repository(batchSize = 2)

        val first = repository.fetchBatch(BatchKeyEvent::class.java)
        assertEquals(
            listOf(n1, n2),
            first.map { it.id },
            "events without a batch key must be fetched as their own capped null-key group"
        )

        val second = repository.fetchBatch(BatchKeyEvent::class.java)
        assertEquals(listOf(a1), second.map { it.id })
        assertEquals(
            emptyList(),
            repository.fetchBatch(BatchKeyEvent::class.java),
            "the pending null-key sibling must stay unclaimed while the null-key group is processing"
        )

        first.forEach { repository.markAsProcessed(it) }

        assertEquals(
            listOf(n3),
            repository.fetchBatch(BatchKeyEvent::class.java).map { it.id },
            "the null-key claim must be released once the group completes"
        )
    }

    @Test
    fun `unannotated fetch returns mixed keys capped by the batch size`() = runBlocking {
        val a1 = insertLegacyEvent("account-a", baseTime)
        val b1 = insertLegacyEvent("account-b", baseTime.plusSeconds(1))
        val a2 = insertLegacyEvent("account-a", baseTime.plusSeconds(2))
        val repository = repository(batchSize = 2)

        val batch = repository.fetchBatch(LegacyEvent::class.java)

        assertEquals(
            listOf(a1, b1),
            batch.map { it.id },
            "the default fetch path may return mixed batch keys up to the batch size"
        )
        assertEquals(EventStatus.PROCESSING, statusOf(a1, table = "legacy_events"))
        assertEquals(EventStatus.PROCESSING, statusOf(b1, table = "legacy_events"))
        assertEquals(EventStatus.PENDING, statusOf(a2, table = "legacy_events"))
    }

    private val baseTime: ZonedDateTime = ZonedDateTime.parse("2026-01-01T00:00:00Z")

    private fun repository(batchSize: Int, maxAttempts: Int = 3): R2dbcEventRepository = R2dbcEventRepository(
        template = template,
        properties = TransactionalProperties(
            polling = TransactionalProperties.Polling(batchSize = batchSize),
            retry = TransactionalProperties.Retry(maxAttempts = maxAttempts)
        ),
        transactionalOperator = TransactionalOperator.create(transactionManager),
        strategiesByEventType = emptyMap()
    )

    private fun insertBatchEvent(accountId: String?, createdAt: ZonedDateTime): UUID = insert(
        table = "batch_key_events",
        accountId = accountId,
        createdAt = createdAt
    )

    private fun insertLegacyEvent(accountId: String, createdAt: ZonedDateTime): UUID = insert(
        table = "legacy_events",
        accountId = accountId,
        createdAt = createdAt
    )

    private fun insert(table: String, accountId: String?, createdAt: ZonedDateTime): UUID {
        val id = UUID.randomUUID()
        var spec = client.sql(
            "INSERT INTO $table (id, status, created_at, retry_count, account_id, payload) " +
                "VALUES (:id, :status, :createdAt, 0, :accountId, 'payload')"
        )
            .bind("id", id)
            .bind("status", EventStatus.PENDING.name)
            .bind("createdAt", createdAt)
        spec = if (accountId == null) {
            spec.bindNull("accountId", String::class.java)
        } else {
            spec.bind("accountId", accountId)
        }
        runBlocking { spec.fetch().rowsUpdated().awaitSingle() }
        return id
    }

    private fun execute(sql: String): Unit = runBlocking {
        client.sql(sql).fetch().rowsUpdated().awaitSingle()
    }

    private fun statusOf(id: UUID, table: String = "batch_key_events"): EventStatus = EventStatus.valueOf(
        runBlocking {
            client.sql("SELECT status FROM $table WHERE id = :id")
                .bind("id", id)
                .map { row, _ -> row.get("status", String::class.java)!! }
                .one()
                .awaitSingle()
        }
    )

    private fun retryCountOf(id: UUID): Int = runBlocking {
        client.sql("SELECT retry_count FROM batch_key_events WHERE id = :id")
            .bind("id", id)
            .map { row, _ -> row.get("retry_count", Int::class.javaObjectType)!! }
            .one()
            .awaitSingle()
    }

    private fun countRows(condition: String, table: String = "batch_key_events"): Int = runBlocking {
        client.sql("SELECT COUNT(*) AS cnt FROM $table WHERE $condition")
            .map { row, _ -> row.get("cnt", Long::class.javaObjectType)!! }
            .one()
            .awaitSingle()
            .toInt()
    }

    companion object {
        val postgres: PostgresContainer = PostgresContainer()

        @BeforeAll
        @JvmStatic
        fun startDatabase() {
            postgres.start()
        }

        @AfterAll
        @JvmStatic
        fun stopDatabase() {
            postgres.stop()
        }

        private const val CREATE_BATCH_KEY_TABLE = """
            CREATE TABLE IF NOT EXISTS batch_key_events (
                id UUID PRIMARY KEY,
                status VARCHAR(32) NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE NOT NULL,
                updated_at TIMESTAMP WITH TIME ZONE,
                retry_count INT NOT NULL DEFAULT 0,
                last_attempt_at TIMESTAMP WITH TIME ZONE,
                next_retry_at TIMESTAMP WITH TIME ZONE,
                account_id VARCHAR(255),
                payload VARCHAR(255) NOT NULL
            )
        """

        private const val CREATE_LEGACY_TABLE = """
            CREATE TABLE IF NOT EXISTS legacy_events (
                id UUID PRIMARY KEY,
                status VARCHAR(32) NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE NOT NULL,
                updated_at TIMESTAMP WITH TIME ZONE,
                retry_count INT NOT NULL DEFAULT 0,
                last_attempt_at TIMESTAMP WITH TIME ZONE,
                next_retry_at TIMESTAMP WITH TIME ZONE,
                account_id VARCHAR(255) NOT NULL,
                payload VARCHAR(255) NOT NULL
            )
        """
    }
}

/** Shared PostgreSQL instance for the R2DBC integration suite. */
class PostgresContainer : PostgreSQLContainer<PostgresContainer>(DockerImageName.parse("postgres:16-alpine"))

@Configuration(proxyBeanMethods = false)
@EnableAutoConfiguration
class R2dbcPostgresTestApp {

    @Bean
    fun connectionFactory(): ConnectionFactory {
        val postgres = R2dbcEventRepositoryPostgresIntegrationTest.postgres
        return ConnectionFactories.get(
            ConnectionFactoryOptions.parse("r2dbc:postgresql://${postgres.host}:${postgres.getMappedPort(5432)}/${postgres.databaseName}")
                .mutate()
                .option(ConnectionFactoryOptions.USER, postgres.username)
                .option(ConnectionFactoryOptions.PASSWORD, postgres.password)
                .build()
        )
    }
}

@Table("batch_key_events")
private class BatchKeyEvent(
    @BatchKey val accountId: String?,
    val payload: String = "payload"
) : BaseEvent()

@Table("legacy_events")
private class LegacyEvent(
    val accountId: String,
    val payload: String = "payload"
) : BaseEvent()
