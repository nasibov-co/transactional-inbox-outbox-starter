package com.fnasibov.transactional.inbox.outbox.starter.r2dbc.domain

import com.fnasibov.transactional.inbox.outbox.core.api.model.BaseEvent
import com.fnasibov.transactional.inbox.outbox.core.api.model.EventStatus
import com.fnasibov.transactional.inbox.outbox.core.configuration.TransactionalProperties
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.data.relational.core.mapping.Table
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.r2dbc.core.FetchSpec
import org.springframework.transaction.reactive.TransactionalOperator
import reactor.core.publisher.Mono
import java.time.Duration
import java.util.UUID
import kotlin.test.assertEquals

class BaseEventRepositoryTest {

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

    private fun repositoryWithProperties(
        properties: TransactionalProperties,
        bindings: MutableMap<String, Any?>
    ): BaseEventRepository {
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

        return BaseEventRepository(
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
}
