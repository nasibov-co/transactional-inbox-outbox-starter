package com.fnasibov.transactional.inbox.outbox.core.domain

import com.fnasibov.transactional.inbox.outbox.core.api.BatchEventHandler
import com.fnasibov.transactional.inbox.outbox.core.api.model.BaseEvent
import com.fnasibov.transactional.inbox.outbox.core.api.model.BatchEventOutcome
import com.fnasibov.transactional.inbox.outbox.core.api.model.BatchResult
import com.fnasibov.transactional.inbox.outbox.core.api.model.Event
import com.fnasibov.transactional.inbox.outbox.core.api.model.EventStatus
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.test.assertEquals
import kotlin.test.assertSame
import kotlin.test.assertTrue

class BatchEventWorkerTest {

    @Test
    fun `missing batch handler moves every event straight to dead letter`() {
        val events: List<Event> = listOf(TestBatchEvent(), TestBatchEvent())
        val repository = RecordingRepository()
        val registry = SimpleMeterRegistry()

        runBatchWorker(
            batches = listOf(events),
            batchHandlers = emptyMap(),
            repository = repository,
            metrics = EventProcessingMetrics(registry)
        )

        assertEquals(events, repository.deadLettered.toList())
        assertTrue(repository.processed.isEmpty())
        assertTrue(repository.failed.isEmpty())
        assertEquals(2.0, registry.get("transactional.events.dead_letter").counter().count())
    }

    @Test
    fun `processed marking failure routes the event through the failure lifecycle`() {
        val broken = TestBatchEvent()
        val ok = TestBatchEvent()
        val markingError = IllegalStateException("status update failed")
        val handler = RecordingBatchHandler()
        val repository = RecordingRepository(
            processedError = { event -> if (event === broken) markingError else null }
        )

        runBatchWorker(
            batches = listOf(listOf(broken, ok)),
            batchHandlers = batchHandlersOf(handler),
            repository = repository
        )

        assertEquals(listOf<Event>(ok), repository.processed.toList())
        assertEquals(listOf<Event>(broken), repository.failed.toList())
        assertTrue(repository.deadLettered.isEmpty())
        assertTrue(handler.deadLetterCalls.isEmpty())
    }

    @Test
    fun `failure recording errors are contained and later batches still process`() {
        val failing = TestBatchEvent()
        val succeeding = TestBatchEvent()
        val handler = RecordingBatchHandler(
            outcome = { events ->
                if (events.first() === failing) {
                    throw IllegalStateException("batch publish failed")
                }
                BatchResult.allProcessed(events)
            }
        )
        val repository = RecordingRepository(
            failedError = { IllegalStateException("status update failed") }
        )

        runBatchWorker(
            batches = listOf(listOf(failing), listOf(succeeding)),
            batchHandlers = batchHandlersOf(handler),
            repository = repository
        )

        assertEquals(listOf<Event>(succeeding), repository.processed.toList())
        assertTrue(handler.deadLetterCalls.isEmpty())
    }

    @Test
    fun `failing dead letter callback does not stop the worker`() {
        val failing = TestBatchEvent()
        val succeeding = TestBatchEvent()
        val failure = IllegalStateException("batch publish failed")
        val handler = RecordingBatchHandler(
            outcome = { events ->
                if (events.first() === failing) {
                    throw failure
                }
                BatchResult.allProcessed(events)
            },
            onDeadLetter = { _, _ -> throw IllegalStateException("dead letter sink unavailable") }
        )
        val repository = RecordingRepository(failureStatus = { EventStatus.DEAD_LETTER })

        runBatchWorker(
            batches = listOf(listOf(failing), listOf(succeeding)),
            batchHandlers = batchHandlersOf(handler),
            repository = repository
        )

        assertEquals(listOf<Event>(succeeding), repository.processed.toList())
        val (deadLettered, error) = handler.deadLetterCalls.single()
        assertEquals(listOf<Event>(failing), deadLettered)
        assertSame(failure, error)
    }

    @Test
    fun `batch result missing an outcome id fails the whole batch`() {
        val acknowledged = TestBatchEvent()
        val omitted = TestBatchEvent()
        val handler = RecordingBatchHandler(
            outcome = { events ->
                BatchResult.of(mapOf(events.first().id!! to BatchEventOutcome.PROCESSED))
            }
        )
        val repository = RecordingRepository()

        runBatchWorker(
            batches = listOf(listOf(acknowledged, omitted)),
            batchHandlers = batchHandlersOf(handler),
            repository = repository
        )

        assertTrue(repository.processed.isEmpty())
        assertEquals(listOf<Event>(acknowledged, omitted), repository.failed.toList())
        assertTrue(repository.deadLettered.isEmpty())
    }

    @Test
    fun `batch result with an unknown outcome id fails the whole batch`() {
        val first = TestBatchEvent()
        val second = TestBatchEvent()
        val stranger = TestBatchEvent()
        val handler = RecordingBatchHandler(
            outcome = { events ->
                BatchResult.of(
                    mapOf(
                        events[0].id!! to BatchEventOutcome.PROCESSED,
                        events[1].id!! to BatchEventOutcome.PROCESSED,
                        stranger.id!! to BatchEventOutcome.RETRY
                    )
                )
            }
        )
        val repository = RecordingRepository()

        runBatchWorker(
            batches = listOf(listOf(first, second)),
            batchHandlers = batchHandlersOf(handler),
            repository = repository
        )

        assertTrue(repository.processed.isEmpty())
        assertEquals(listOf<Event>(first, second), repository.failed.toList())
        assertTrue(repository.deadLettered.isEmpty())
    }

    private fun batchHandlersOf(
        handler: BatchEventHandler<out Event>
    ): Map<Class<out Event>, List<BatchEventHandler<out Event>>> =
        mapOf(handler.supportedEventType() to listOf(handler))

    private fun runBatchWorker(
        batches: List<List<Event>>,
        batchHandlers: Map<Class<out Event>, List<BatchEventHandler<out Event>>>,
        repository: EventRepository,
        metrics: EventProcessingMetrics? = null
    ) {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        try {
            val channel = Channel<List<Event>>(capacity = Channel.UNLIMITED)
            val jobs = BatchEventWorker(
                batchHandlers = batchHandlers,
                repository = repository,
                concurrency = 1,
                channel = channel,
                scope = scope,
                metrics = metrics
            ).start()
            runBlocking {
                batches.forEach { channel.send(it) }
                channel.close()
                jobs.joinAll()
            }
        } finally {
            scope.cancel()
        }
    }

    private class TestBatchEvent : BaseEvent(id = UUID.randomUUID())

    private class RecordingBatchHandler(
        private val outcome: (List<TestBatchEvent>) -> BatchResult = { BatchResult.allProcessed(it) },
        private val onDeadLetter: (List<TestBatchEvent>, Throwable) -> Unit = { _, _ -> }
    ) : BatchEventHandler<TestBatchEvent> {

        val deadLetterCalls = CopyOnWriteArrayList<Pair<List<TestBatchEvent>, Throwable>>()

        override fun supportedEventType(): Class<TestBatchEvent> = TestBatchEvent::class.java

        override suspend fun handleBatch(events: List<TestBatchEvent>): BatchResult = outcome(events)

        override suspend fun handleDeadLetter(events: List<TestBatchEvent>, error: Throwable) {
            deadLetterCalls += events to error
            onDeadLetter(events, error)
        }
    }

    private class RecordingRepository(
        private val failureStatus: (Event) -> EventStatus = { EventStatus.FAILED },
        private val processedError: (Event) -> Throwable? = { null },
        private val failedError: (Event) -> Throwable? = { null }
    ) : EventRepository {

        val processed = CopyOnWriteArrayList<Event>()
        val failed = CopyOnWriteArrayList<Event>()
        val deadLettered = CopyOnWriteArrayList<Event>()

        override suspend fun <E : Event> fetchBatch(eventType: Class<E>): List<E> = emptyList()

        override suspend fun <E : Event> markAsProcessed(event: E) {
            processedError(event)?.let { throw it }
            processed += event
        }

        override suspend fun <E : Event> markAsDeadLetter(event: E) {
            deadLettered += event
        }

        override suspend fun <E : Event> markAsFailed(event: E): EventStatus {
            failedError(event)?.let { throw it }
            failed += event
            return failureStatus(event)
        }
    }
}
