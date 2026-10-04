package com.fnasibov.transactional.inbox.outbox.jdbc

import com.fnasibov.transactional.inbox.outbox.core.api.model.BaseEvent
import com.fnasibov.transactional.inbox.outbox.core.api.model.BatchKey
import com.fnasibov.transactional.inbox.outbox.core.api.model.EventStatus
import com.fnasibov.transactional.inbox.outbox.core.configuration.TransactionalProperties
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.springframework.data.jdbc.core.JdbcAggregateOperations
import org.springframework.data.relational.core.mapping.Table
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations
import org.springframework.jdbc.core.namedparam.SqlParameterSource
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.support.TransactionTemplate
import java.time.OffsetDateTime
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class JdbcEventRepositoryTest {

    @Test
    fun `markAsFailed keeps event failed while retries remain`() = runBlocking {
        val sql = slot<String>()
        val parameters = slot<SqlParameterSource>()
        val jdbc = mockk<NamedParameterJdbcOperations>()
        every { jdbc.update(capture(sql), capture(parameters)) } returns 1
        val repository = repository(
            jdbc = jdbc,
            properties = TransactionalProperties(
                retry = TransactionalProperties.Retry(maxAttempts = 2)
            )
        )

        val status = repository.markAsFailed(TestEvent(retryCount = 0))

        assertEquals(EventStatus.FAILED, status)
        assertEquals(EventStatus.FAILED.name, parameters.captured.getValue("status"))
        assertEquals(1, parameters.captured.getValue("retryCount"))
        assertEquals(true, sql.captured.contains("next_retry_at = :nextRetryAt"))
    }

    @Test
    fun `markAsFailed moves event to dead letter at retry limit`() = runBlocking {
        val sql = slot<String>()
        val parameters = slot<SqlParameterSource>()
        val jdbc = mockk<NamedParameterJdbcOperations>()
        every { jdbc.update(capture(sql), capture(parameters)) } returns 1
        val repository = repository(
            jdbc = jdbc,
            properties = TransactionalProperties(
                retry = TransactionalProperties.Retry(maxAttempts = 1)
            )
        )

        val status = repository.markAsFailed(TestEvent(retryCount = 0))

        assertEquals(EventStatus.DEAD_LETTER, status)
        assertEquals(EventStatus.DEAD_LETTER.name, parameters.captured.getValue("status"))
        assertEquals(true, sql.captured.contains("next_retry_at = NULL"))
    }

    @Test
    fun `markAsDeadLetter sets dead letter status and clears scheduled retry`() = runBlocking {
        val sql = slot<String>()
        val parameters = slot<SqlParameterSource>()
        val jdbc = mockk<NamedParameterJdbcOperations>()
        every { jdbc.update(capture(sql), capture(parameters)) } returns 1
        val repository = repository(jdbc = jdbc, properties = TransactionalProperties())
        val event = TestEvent(retryCount = 0)

        repository.markAsDeadLetter(event)

        assertEquals(EventStatus.DEAD_LETTER.name, parameters.captured.getValue("status"))
        assertEquals(event.id, parameters.captured.getValue("id"))
        val updatedAt = parameters.captured.getValue("updatedAt") as OffsetDateTime
        assertTrue(
            updatedAt.isAfter(OffsetDateTime.now().minusMinutes(1)),
            "updated_at must be refreshed when the event is dead lettered"
        )
        assertTrue(sql.captured.contains("next_retry_at = NULL"))
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
    ): JdbcEventRepository {
        val ids = events.map { requireNotNull(it.id) }

        val jdbc = mockk<NamedParameterJdbcOperations>(relaxed = true)
        every {
            jdbc.queryForList(any<String>(), any<SqlParameterSource>(), any<Class<*>>())
        } answers { ids }

        val aggregates = mockk<JdbcAggregateOperations>(relaxed = true)
        every { aggregates.findAllById(any(), any<Class<*>>()) } answers { events }

        return JdbcEventRepository(
            jdbc = jdbc,
            aggregates = aggregates,
            transactionTemplate = TransactionTemplate(
                mockk<PlatformTransactionManager>(relaxed = true)
            ),
            properties = properties,
            strategiesByEventType = emptyMap()
        )
    }

    private fun repository(
        jdbc: NamedParameterJdbcOperations,
        properties: TransactionalProperties
    ): JdbcEventRepository = JdbcEventRepository(
        jdbc = jdbc,
        aggregates = mockk<JdbcAggregateOperations>(),
        transactionTemplate = TransactionTemplate(mockk<PlatformTransactionManager>()),
        properties = properties,
        strategiesByEventType = emptyMap()
    )

    @Table("test_events")
    private class TestEvent(
        retryCount: Int
    ) : BaseEvent(
        id = UUID.randomUUID(),
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
