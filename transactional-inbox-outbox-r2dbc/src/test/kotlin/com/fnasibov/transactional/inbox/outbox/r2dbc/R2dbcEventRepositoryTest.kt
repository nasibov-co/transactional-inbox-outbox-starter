package com.fnasibov.transactional.inbox.outbox.r2dbc

import com.fnasibov.transactional.inbox.outbox.core.api.model.BaseEvent
import com.fnasibov.transactional.inbox.outbox.core.api.model.BatchKey
import com.fnasibov.transactional.inbox.outbox.core.api.model.EventStatus
import com.fnasibov.transactional.inbox.outbox.core.configuration.TransactionalProperties
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.r2dbc.spi.Row
import io.r2dbc.spi.RowMetadata
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.data.relational.core.mapping.Table
import org.springframework.data.relational.core.query.Query
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.r2dbc.core.FetchSpec
import org.springframework.r2dbc.core.RowsFetchSpec
import org.springframework.transaction.ReactiveTransaction
import org.springframework.transaction.reactive.TransactionCallback
import org.springframework.transaction.reactive.TransactionalOperator
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.Duration
import java.util.UUID
import java.util.function.BiFunction
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class R2dbcEventRepositoryTest {

    @Test
    fun `markAsFailed keeps event failed when event type retry allows more attempts`() {
        val bindings = mutableMapOf<String, Any?>()
        val repository = repositoryWithProperties(
            properties = TransactionalProperties(
                processing = TransactionalProperties.Processing(
                    eventTypes = listOf(
                        TransactionalProperties.EventTypeProcessing(
                            eventType = RetryOverrideEvent::class.java.simpleName,
                            retry = TransactionalProperties.EventTypeRetry(
                                maxAttempts = 5
                            )
                        )
                    )
                ),
                retry = TransactionalProperties.Retry(maxAttempts = 1)
            ),
            bindings = bindings
        )

        val status = runBlocking {
            repository.markAsFailed(
                RetryOverrideEvent(
                    id = UUID.randomUUID(),
                    retryCount = 0
                )
            )
        }

        assertEquals(EventStatus.FAILED, status)
        assertEquals(EventStatus.FAILED.name, bindings["status"])
        assertEquals(1, bindings["retryCount"])
    }

    @Test
    fun `markAsFailed uses global retry when event type has no override`() {
        val bindings = mutableMapOf<String, Any?>()
        val repository = repositoryWithProperties(
            properties = TransactionalProperties(
                retry = TransactionalProperties.Retry(maxAttempts = 1)
            ),
            bindings = bindings
        )

        val status = runBlocking {
            repository.markAsFailed(
                GlobalRetryEvent(
                    id = UUID.randomUUID(),
                    retryCount = 0
                )
            )
        }

        assertEquals(EventStatus.DEAD_LETTER, status)
        assertEquals(EventStatus.DEAD_LETTER.name, bindings["status"])
        assertEquals(1, bindings["retryCount"])
    }

    @Test
    fun `fetchBatch groups a batch by BatchKey for annotated event models`() = runBlocking {
        val first = BatchKeyEvent(accountId = "account-a")
        val second = BatchKeyEvent(accountId = "account-b")
        val repository = transactionalRepository(listOf(first, second))

        val batch = repository.fetchBatch(BatchKeyEvent::class.java)

        assertTrue(batch.isNotEmpty(), "annotated fetch must return at least one event")
        val keys = batch.map { it.accountId }.distinct()
        assertEquals(
            1,
            keys.size,
            "batch must be homogeneous by @BatchKey, but contained keys $keys"
        )
    }

    @Test
    fun `fetchBatch caps an annotated batch at the configured batch size`() = runBlocking {
        val first = BatchKeyEvent(accountId = "account-a")
        val second = BatchKeyEvent(accountId = "account-a")
        val repository = transactionalRepository(
            events = listOf(first, second),
            properties = TransactionalProperties(
                polling = TransactionalProperties.Polling(batchSize = 1)
            )
        )

        val batch = repository.fetchBatch(BatchKeyEvent::class.java)

        assertEquals(listOf(first), batch)
    }

    @Test
    fun `fetchBatch keeps legacy mixed batch keys for models without BatchKey`() = runBlocking {
        val first = LegacyEvent(accountId = "account-a")
        val second = LegacyEvent(accountId = "account-b")
        val repository = transactionalRepository(listOf(first, second))

        val batch = repository.fetchBatch(LegacyEvent::class.java)

        assertEquals(2, batch.size)
        assertEquals(
            setOf("account-a", "account-b"),
            batch.map { it.accountId }.toSet()
        )
    }

    private fun transactionalRepository(
        events: List<BaseEvent>,
        properties: TransactionalProperties = TransactionalProperties()
    ): R2dbcEventRepository {
        val ids = events.map { requireNotNull(it.id) }

        val idsFetch = mockk<RowsFetchSpec<UUID>>()
        every { idsFetch.all() } returns Flux.fromIterable(ids)

        val rowsUpdatedFetch = mockk<FetchSpec<Map<String, Any>>>()
        every { rowsUpdatedFetch.rowsUpdated() } returns Mono.just(1L)

        val executeSpec = mockk<DatabaseClient.GenericExecuteSpec>(relaxed = true)
        every { executeSpec.bind(any<String>(), any()) } returns executeSpec
        every {
            executeSpec.map(any<BiFunction<Row, RowMetadata, UUID>>())
        } returns idsFetch
        every { executeSpec.fetch() } returns rowsUpdatedFetch

        val databaseClient = mockk<DatabaseClient>()
        every { databaseClient.sql(any<String>()) } returns executeSpec

        val template = mockk<R2dbcEntityTemplate>()
        every { template.databaseClient } returns databaseClient
        every {
            template.select(any<Query>(), any<Class<*>>())
        } answers { Flux.fromIterable(events) }

        val transactionalOperator = mockk<TransactionalOperator>()
        every { transactionalOperator.execute(any<TransactionCallback<*>>()) } answers {
            val callback = firstArg<TransactionCallback<List<BaseEvent>>>()
            Flux.from(callback.doInTransaction(mockk<ReactiveTransaction>(relaxed = true)))
        }

        return R2dbcEventRepository(
            template = template,
            properties = properties,
            transactionalOperator = transactionalOperator,
            strategiesByEventType = emptyMap()
        )
    }

    private fun repositoryWithProperties(
        properties: TransactionalProperties,
        bindings: MutableMap<String, Any?>
    ): R2dbcEventRepository {
        val rowsUpdated = mockk<FetchSpec<Map<String, Any>>>()
        every { rowsUpdated.rowsUpdated() } returns Mono.just(1)

        val statement = mockk<DatabaseClient.GenericExecuteSpec>()
        every { statement.bind(any<String>(), any()) } answers {
            bindings[firstArg()] = secondArg()
            statement
        }
        every { statement.fetch() } returns rowsUpdated

        val sql = slot<String>()
        val databaseClient = mockk<DatabaseClient>()
        every { databaseClient.sql(capture(sql)) } returns statement

        val template = mockk<R2dbcEntityTemplate>()
        every { template.databaseClient } returns databaseClient

        return R2dbcEventRepository(
            template = template,
            properties = properties,
            transactionalOperator = mockk<TransactionalOperator>(),
            strategiesByEventType = emptyMap()
        )
    }

    @Table("retry_override_events")
    private class RetryOverrideEvent(
        id: UUID,
        retryCount: Int
    ) : BaseEvent(
        id = id,
        retryCount = retryCount
    )

    @Table("global_retry_events")
    private class GlobalRetryEvent(
        id: UUID,
        retryCount: Int
    ) : BaseEvent(
        id = id,
        retryCount = retryCount
    )

    @Table("batch_key_events")
    private class BatchKeyEvent(
        @BatchKey val accountId: String,
        id: UUID = UUID.randomUUID()
    ) : BaseEvent(id = id)

    @Table("legacy_events")
    private class LegacyEvent(
        val accountId: String,
        id: UUID = UUID.randomUUID()
    ) : BaseEvent(id = id)
}
