package com.fnasibov.transactional.inbox.outbox.core.domain

import com.fnasibov.transactional.inbox.outbox.core.api.BatchEventHandler
import com.fnasibov.transactional.inbox.outbox.core.api.EventHandler
import com.fnasibov.transactional.inbox.outbox.core.api.model.BaseEvent
import com.fnasibov.transactional.inbox.outbox.core.api.model.BatchEventOutcome
import com.fnasibov.transactional.inbox.outbox.core.api.model.BatchKey
import com.fnasibov.transactional.inbox.outbox.core.api.model.BatchResult
import com.fnasibov.transactional.inbox.outbox.core.api.model.Event
import com.fnasibov.transactional.inbox.outbox.core.api.model.EventStatus
import com.fnasibov.transactional.inbox.outbox.core.configuration.TransactionalProperties
import com.fnasibov.transactional.inbox.outbox.core.domain.exception.BatchResultValidationException
import com.fnasibov.transactional.inbox.outbox.core.domain.exception.BatchRetryRequestedException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertSame
import kotlin.test.assertTrue

class EventProcessorBatchHandlerTest {

    @Test
    fun `batch handler receives a fetched key group in one call and marks every event processed`() {
        val first = TestBatchEvent(accountId = "account-1", amount = 10)
        val second = TestBatchEvent(accountId = "account-1", amount = 20)
        val handler = RecordingBatchHandler()
        val repository = RecordingRepository(batches = listOf(listOf(first, second)))

        withPipeline(repository, batchHandlers = batchHandlersOf(handler)) {
            awaitUntil { repository.processed.size == 2 }

            assertEquals(listOf(listOf(first, second)), handler.invocations.toList())
            assertEquals(listOf(first, second), repository.processed.toList())
            assertTrue(repository.failed.isEmpty())
            assertTrue(repository.deadLettered.isEmpty())
        }
    }

    @Test
    fun `partial outcomes mark successful events processed and route retryable events through the retry lifecycle`() {
        val successful = TestBatchEvent(accountId = "account-1", amount = 10)
        val retryable = TestBatchEvent(accountId = "account-1", amount = 20)
        val handler = RecordingBatchHandler {
            BatchResult.of(
                mapOf(
                    successful.id!! to BatchEventOutcome.PROCESSED,
                    retryable.id!! to BatchEventOutcome.RETRY
                )
            )
        }
        val repository = RecordingRepository(batches = listOf(listOf(successful, retryable)))

        withPipeline(repository, batchHandlers = batchHandlersOf(handler)) {
            awaitUntil { repository.failed.size == 1 }

            assertEquals(listOf(successful), repository.processed.toList())
            assertEquals(listOf(retryable), repository.failed.toList())
            assertTrue(repository.deadLettered.isEmpty())
            assertTrue(handler.deadLetterCalls.isEmpty())
        }
    }

    @Test
    fun `retry outcome dead letters the event with the reported failure when retries are exhausted`() {
        val successful = TestBatchEvent(accountId = "account-1", amount = 10)
        val retryable = TestBatchEvent(accountId = "account-1", amount = 20)
        val handler = RecordingBatchHandler {
            BatchResult.of(
                mapOf(
                    successful.id!! to BatchEventOutcome.PROCESSED,
                    retryable.id!! to BatchEventOutcome.RETRY
                )
            )
        }
        val repository = RecordingRepository(
            batches = listOf(listOf(successful, retryable)),
            failureStatus = { EventStatus.DEAD_LETTER }
        )

        withPipeline(repository, batchHandlers = batchHandlersOf(handler)) {
            awaitUntil { handler.deadLetterCalls.size == 1 }

            val (deadLettered, error) = handler.deadLetterCalls.single()
            assertEquals(listOf(retryable), deadLettered)
            assertTrue(error is BatchRetryRequestedException)
            assertEquals(listOf(successful), repository.processed.toList())
        }
    }

    @Test
    fun `throwing from handleBatch retries the whole batch`() {
        val first = TestBatchEvent(accountId = "account-1", amount = 10)
        val second = TestBatchEvent(accountId = "account-1", amount = 20)
        val handler = RecordingBatchHandler { throw IllegalStateException("batch publish failed") }
        val repository = RecordingRepository(batches = listOf(listOf(first, second)))

        withPipeline(repository, batchHandlers = batchHandlersOf(handler)) {
            awaitUntil { repository.failed.size == 2 }

            assertTrue(repository.processed.isEmpty())
            assertEquals(setOf(first, second), repository.failed.toSet())
            assertTrue(repository.deadLettered.isEmpty())
            assertTrue(handler.deadLetterCalls.isEmpty())
        }
    }

    @Test
    fun `throwing from handleBatch dead letters the exhausted batch with the original error`() {
        val first = TestBatchEvent(accountId = "account-1", amount = 10)
        val second = TestBatchEvent(accountId = "account-1", amount = 20)
        val failure = IllegalStateException("batch publish failed")
        val handler = RecordingBatchHandler { throw failure }
        val repository = RecordingRepository(
            batches = listOf(listOf(first, second)),
            failureStatus = { EventStatus.DEAD_LETTER }
        )

        withPipeline(repository, batchHandlers = batchHandlersOf(handler)) {
            awaitUntil { handler.deadLetterCalls.size == 1 }

            val (deadLettered, error) = handler.deadLetterCalls.single()
            assertEquals(listOf(first, second), deadLettered)
            assertSame(failure, error)
        }
    }

    @Test
    fun `incomplete outcome map retries the whole batch and never strands events`() {
        val acknowledged = TestBatchEvent(accountId = "account-1", amount = 10)
        val omitted = TestBatchEvent(accountId = "account-1", amount = 20)
        val handler = RecordingBatchHandler {
            BatchResult.of(mapOf(acknowledged.id!! to BatchEventOutcome.PROCESSED))
        }
        val repository = RecordingRepository(batches = listOf(listOf(acknowledged, omitted)))

        withPipeline(repository, batchHandlers = batchHandlersOf(handler)) {
            awaitUntil { repository.failed.size == 2 }

            assertTrue(repository.processed.isEmpty())
            assertEquals(setOf(acknowledged, omitted), repository.failed.toSet())
        }
    }

    @Test
    fun `invalid outcome map is reported through dead letter when retries are exhausted`() {
        val acknowledged = TestBatchEvent(accountId = "account-1", amount = 10)
        val omitted = TestBatchEvent(accountId = "account-1", amount = 20)
        val handler = RecordingBatchHandler {
            BatchResult.of(mapOf(acknowledged.id!! to BatchEventOutcome.PROCESSED))
        }
        val repository = RecordingRepository(
            batches = listOf(listOf(acknowledged, omitted)),
            failureStatus = { EventStatus.DEAD_LETTER }
        )

        withPipeline(repository, batchHandlers = batchHandlersOf(handler)) {
            awaitUntil { handler.deadLetterCalls.size == 1 }

            val (deadLettered, error) = handler.deadLetterCalls.single()
            assertEquals(setOf(acknowledged, omitted), deadLettered.toSet())
            assertTrue(error is BatchResultValidationException)
        }
    }

    @Test
    fun `outcome ids outside the batch retry the whole batch`() {
        val first = TestBatchEvent(accountId = "account-1", amount = 10)
        val second = TestBatchEvent(accountId = "account-1", amount = 20)
        val stranger = TestBatchEvent(accountId = "account-2", amount = 30)
        val handler = RecordingBatchHandler {
            BatchResult.of(
                mapOf(
                    first.id!! to BatchEventOutcome.PROCESSED,
                    second.id!! to BatchEventOutcome.PROCESSED,
                    stranger.id!! to BatchEventOutcome.RETRY
                )
            )
        }
        val repository = RecordingRepository(batches = listOf(listOf(first, second)))

        withPipeline(repository, batchHandlers = batchHandlersOf(handler)) {
            awaitUntil { repository.failed.size == 2 }

            assertTrue(repository.processed.isEmpty())
            assertEquals(setOf(first, second), repository.failed.toSet())
        }
    }

    @Test
    fun `rejects mixing regular and batch handlers for one event type`() {
        val repository = RecordingRepository(batches = emptyList())

        val error = assertFailsWith<IllegalArgumentException> {
            EventProcessor(
                handlers = mapOf(TestBatchEvent::class.java to listOf(RecordingSingleHandler())),
                batchHandlers = batchHandlersOf(RecordingBatchHandler()),
                repository = repository,
                properties = TransactionalProperties(),
                scope = CoroutineScope(SupervisorJob() + Dispatchers.Default),
                metrics = null
            )
        }

        assertTrue(error.message.orEmpty().contains("BatchEventHandler"))
    }

    @Test
    fun `regular event handlers still receive one callback per event`() {
        val first = TestBatchEvent(accountId = "account-1", amount = 10)
        val second = TestBatchEvent(accountId = "account-1", amount = 20)
        val handler = RecordingSingleHandler()
        val repository = RecordingRepository(batches = listOf(listOf(first, second)))

        withPipeline(repository, handlers = mapOf(TestBatchEvent::class.java to listOf(handler))) {
            awaitUntil { repository.processed.size == 2 }

            // Regular handlers are invoked concurrently by the worker pool, so
            // callback order is not guaranteed; exactly one callback per event is.
            assertEquals(2, handler.received.size)
            assertEquals(setOf(first, second), handler.received.toSet())
            assertTrue(repository.failed.isEmpty())
        }
    }

    private fun batchHandlersOf(
        vararg handlers: BatchEventHandler<out Event>
    ): Map<Class<out Event>, List<BatchEventHandler<out Event>>> =
        handlers.groupBy { it.supportedEventType() }

    private fun withPipeline(
        repository: EventRepository,
        handlers: Map<Class<out Event>, List<EventHandler<out Event>>> = emptyMap(),
        batchHandlers: Map<Class<out Event>, List<BatchEventHandler<out Event>>> = emptyMap(),
        testBody: suspend () -> Unit
    ) {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        try {
            val processor = EventProcessor(
                handlers = handlers,
                batchHandlers = batchHandlers,
                repository = repository,
                properties = TransactionalProperties(
                    polling = TransactionalProperties.Polling(
                        activeInterval = Duration.ofMillis(10)
                    ),
                    processing = TransactionalProperties.Processing(
                        shutdownTimeout = Duration.ofSeconds(1)
                    )
                ),
                scope = scope,
                metrics = null
            )
            processor.start()
            runBlocking { testBody() }
        } finally {
            scope.cancel()
        }
    }

    private suspend fun awaitUntil(timeoutMillis: Long = 2_000, condition: () -> Boolean) {
        withTimeout(timeoutMillis) {
            while (!condition()) {
                delay(5)
            }
        }
    }

    private class TestBatchEvent(
        @BatchKey val accountId: String,
        val amount: Long = 0,
        id: UUID? = UUID.randomUUID()
    ) : BaseEvent(id = id)

    private class RecordingBatchHandler(
        private val outcome: (List<TestBatchEvent>) -> BatchResult = { BatchResult.allProcessed(it) }
    ) : BatchEventHandler<TestBatchEvent> {

        val invocations = CopyOnWriteArrayList<List<TestBatchEvent>>()
        val deadLetterCalls = CopyOnWriteArrayList<Pair<List<TestBatchEvent>, Throwable>>()

        override fun supportedEventType(): Class<TestBatchEvent> = TestBatchEvent::class.java

        override suspend fun handleBatch(events: List<TestBatchEvent>): BatchResult {
            invocations += events
            return outcome(events)
        }

        override suspend fun handleDeadLetter(events: List<TestBatchEvent>, error: Throwable) {
            deadLetterCalls += events to error
        }
    }

    private class RecordingSingleHandler : EventHandler<TestBatchEvent> {

        val received = CopyOnWriteArrayList<TestBatchEvent>()

        override fun supportedEventType(): Class<TestBatchEvent> = TestBatchEvent::class.java

        override suspend fun handle(event: TestBatchEvent) {
            received += event
        }

        override suspend fun handleDeadLetter(event: TestBatchEvent, error: Throwable) = Unit
    }

    private class RecordingRepository(
        private val batches: List<List<Event>>,
        private val failureStatus: (Event) -> EventStatus = { EventStatus.FAILED }
    ) : EventRepository {

        val processed = CopyOnWriteArrayList<Event>()
        val failed = CopyOnWriteArrayList<Event>()
        val deadLettered = CopyOnWriteArrayList<Event>()

        private val fetchCount = AtomicInteger()

        @Suppress("UNCHECKED_CAST")
        override suspend fun <E : Event> fetchBatch(eventType: Class<E>): List<E> =
            batches.getOrNull(fetchCount.getAndIncrement()) as List<E>? ?: emptyList()

        override suspend fun <E : Event> markAsProcessed(event: E) {
            processed += event
        }

        override suspend fun <E : Event> markAsDeadLetter(event: E) {
            deadLettered += event
        }

        override suspend fun <E : Event> markAsFailed(event: E): EventStatus {
            failed += event
            return failureStatus(event)
        }
    }
}
