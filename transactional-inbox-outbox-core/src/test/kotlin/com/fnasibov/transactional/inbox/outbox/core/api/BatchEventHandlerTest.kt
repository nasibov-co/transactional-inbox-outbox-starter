package com.fnasibov.transactional.inbox.outbox.core.api

import com.fnasibov.transactional.inbox.outbox.core.api.model.BaseEvent
import com.fnasibov.transactional.inbox.outbox.core.api.model.BatchEventOutcome
import com.fnasibov.transactional.inbox.outbox.core.api.model.BatchKey
import com.fnasibov.transactional.inbox.outbox.core.api.model.BatchResult
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import java.util.UUID
import kotlin.test.assertEquals

class BatchEventHandlerTest {

    @Test
    fun `client declares supported event type like a regular handler`() {
        val handler = RecordingBatchEventHandler()

        assertEquals(TestBatchEvent::class.java, handler.supportedEventType())
    }

    @Test
    fun `client receives the whole batch in a single callback`() = runBlocking {
        val handler = RecordingBatchEventHandler()
        val batch = listOf(
            TestBatchEvent(accountId = "account-1", amount = 10),
            TestBatchEvent(accountId = "account-1", amount = 20)
        )

        handler.handleBatch(batch)

        assertEquals(1, handler.invocations)
        assertEquals(batch, handler.received.single())
    }

    @Test
    fun `client reports per-event outcomes keyed by event id`() = runBlocking {
        val successful = TestBatchEvent(accountId = "account-1", amount = 10)
        val retryable = TestBatchEvent(accountId = "account-1", amount = 20)
        val handler = RecordingBatchEventHandler(
            outcome = {
                BatchResult.of(
                    mapOf(
                        successful.id!! to BatchEventOutcome.PROCESSED,
                        retryable.id!! to BatchEventOutcome.RETRY
                    )
                )
            }
        )

        val result = handler.handleBatch(listOf(successful, retryable))

        assertEquals(
            mapOf(
                successful.id!! to BatchEventOutcome.PROCESSED,
                retryable.id!! to BatchEventOutcome.RETRY
            ),
            result.outcomes
        )
    }

    @Test
    fun `default outcome helpers cover the whole batch`() = runBlocking {
        val batch = listOf(
            TestBatchEvent(accountId = "account-1", amount = 10),
            TestBatchEvent(accountId = "account-1", amount = 20)
        )
        val handler = RecordingBatchEventHandler()

        val result = handler.handleBatch(batch)

        assertEquals(
            batch.associate { it.id!! to BatchEventOutcome.PROCESSED },
            result.outcomes
        )
    }

    @Test
    fun `client receives the whole batch on dead letter`() = runBlocking {
        val handler = RecordingBatchEventHandler()
        val batch = listOf(TestBatchEvent(accountId = "account-1", amount = 10))
        val error = IllegalStateException("processing failed")

        handler.handleDeadLetter(batch, error)

        assertEquals(1, handler.deadLetterInvocations)
        assertEquals(batch, handler.deadLetterBatch)
        assertEquals(error, handler.deadLetterError)
    }

    private class RecordingBatchEventHandler(
        private val outcome: (List<TestBatchEvent>) -> BatchResult = { BatchResult.allProcessed(it) }
    ) : BatchEventHandler<TestBatchEvent> {

        var invocations = 0
        var received: List<List<TestBatchEvent>> = emptyList()

        var deadLetterInvocations = 0
        var deadLetterBatch: List<TestBatchEvent>? = null
        var deadLetterError: Throwable? = null

        override fun supportedEventType(): Class<TestBatchEvent> = TestBatchEvent::class.java

        override suspend fun handleBatch(events: List<TestBatchEvent>): BatchResult {
            invocations++
            received = received + listOf(events)
            return outcome(events)
        }

        override suspend fun handleDeadLetter(events: List<TestBatchEvent>, error: Throwable) {
            deadLetterInvocations++
            deadLetterBatch = events
            deadLetterError = error
        }
    }

    private data class TestBatchEvent(
        @BatchKey val accountId: String,
        val amount: Long
    ) : BaseEvent(id = UUID.randomUUID())
}
