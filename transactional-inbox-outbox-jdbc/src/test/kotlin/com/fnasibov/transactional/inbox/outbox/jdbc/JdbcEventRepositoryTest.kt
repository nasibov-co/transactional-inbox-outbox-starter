package com.fnasibov.transactional.inbox.outbox.jdbc

import com.fnasibov.transactional.inbox.outbox.core.api.model.BaseEvent
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
import java.util.UUID
import kotlin.test.assertEquals

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
}
