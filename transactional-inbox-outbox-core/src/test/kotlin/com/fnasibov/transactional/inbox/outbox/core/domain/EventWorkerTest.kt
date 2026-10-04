package com.fnasibov.transactional.inbox.outbox.core.domain

import com.fnasibov.transactional.inbox.outbox.core.api.EventHandler
import com.fnasibov.transactional.inbox.outbox.core.api.model.BaseEvent
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

class EventWorkerTest {

    @Test
    fun `successful handling marks the event processed and records metrics`() {
        val event = TestEvent()
        val repository = RecordingRepository()
        val registry = SimpleMeterRegistry()

        runWorker(
            events = listOf(event),
            handlers = handlersOf(TestEventHandler()),
            repository = repository,
            metrics = EventProcessingMetrics(registry)
        )

        assertEquals(listOf(event), repository.processed.toList())
        assertTrue(repository.failed.isEmpty())
        assertTrue(repository.deadLettered.isEmpty())
        assertEquals(1.0, registry.get("transactional.events.processed").counter().count())
        assertEquals(1L, registry.get("transactional.events.processing.duration").timer().count())
    }

    @Test
    fun `handler failure marks the event failed without dead letter while retries remain`() {
        val event = TestEvent()
        val failure = IllegalStateException("publish failed")
        val handler = TestEventHandler(onHandle = { throw failure })
        val repository = RecordingRepository(failureStatus = { EventStatus.FAILED })
        val registry = SimpleMeterRegistry()

        runWorker(
            events = listOf(event),
            handlers = handlersOf(handler),
            repository = repository,
            metrics = EventProcessingMetrics(registry)
        )

        assertEquals(listOf(event), repository.failed.toList())
        assertTrue(repository.processed.isEmpty())
        assertTrue(repository.deadLettered.isEmpty())
        assertTrue(handler.deadLetterCalls.isEmpty())
        assertEquals(1.0, registry.get("transactional.events.failed").counter().count())
        assertEquals(0.0, registry.get("transactional.events.dead_letter").counter().count())
    }

    @Test
    fun `exhausted retries dead letter the event with the original failure`() {
        val event = TestEvent()
        val failure = IllegalStateException("publish failed")
        val handler = TestEventHandler(onHandle = { throw failure })
        val repository = RecordingRepository(failureStatus = { EventStatus.DEAD_LETTER })
        val registry = SimpleMeterRegistry()

        runWorker(
            events = listOf(event),
            handlers = handlersOf(handler),
            repository = repository,
            metrics = EventProcessingMetrics(registry)
        )

        assertEquals(listOf(event), repository.failed.toList())
        assertTrue(
            repository.deadLettered.isEmpty(),
            "the repository already reported the terminal state through markAsFailed"
        )
        val (deadLettered, error) = handler.deadLetterCalls.single()
        assertSame(event, deadLettered)
        assertSame(failure, error)
        assertEquals(1.0, registry.get("transactional.events.failed").counter().count())
        assertEquals(1.0, registry.get("transactional.events.dead_letter").counter().count())
    }

    @Test
    fun `missing handler moves the event straight to dead letter`() {
        val event = TestEvent()
        val repository = RecordingRepository()
        val registry = SimpleMeterRegistry()

        runWorker(
            events = listOf(event),
            handlers = emptyMap(),
            repository = repository,
            metrics = EventProcessingMetrics(registry)
        )

        assertEquals(listOf(event), repository.deadLettered.toList())
        assertTrue(repository.processed.isEmpty())
        assertTrue(repository.failed.isEmpty())
        assertEquals(1.0, registry.get("transactional.events.dead_letter").counter().count())
        assertEquals(0.0, registry.get("transactional.events.failed").counter().count())
    }

    @Test
    fun `failing dead letter callback does not stop the worker`() {
        val first = TestEvent()
        val second = TestEvent()
        val handler = TestEventHandler(
            onHandle = { throw IllegalStateException("publish failed") },
            onDeadLetter = { _, _ -> throw IllegalStateException("dead letter sink unavailable") }
        )
        val repository = RecordingRepository(failureStatus = { EventStatus.DEAD_LETTER })

        runWorker(
            events = listOf(first, second),
            handlers = handlersOf(handler),
            repository = repository
        )

        assertEquals(listOf(first, second), repository.failed.toList())
        assertEquals(2, handler.deadLetterCalls.size)
    }

    private fun handlersOf(
        handler: EventHandler<out Event>
    ): Map<Class<out Event>, List<EventHandler<out Event>>> =
        mapOf(handler.supportedEventType() to listOf(handler))

    private fun runWorker(
        events: List<Event>,
        handlers: Map<Class<out Event>, List<EventHandler<out Event>>>,
        repository: EventRepository,
        metrics: EventProcessingMetrics? = null
    ) {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        try {
            val channel = Channel<Event>(capacity = Channel.UNLIMITED)
            val jobs = EventWorker(
                handlers = handlers,
                repository = repository,
                concurrency = 1,
                channel = channel,
                scope = scope,
                metrics = metrics
            ).start()
            runBlocking {
                events.forEach { channel.send(it) }
                channel.close()
                jobs.joinAll()
            }
        } finally {
            scope.cancel()
        }
    }

    private class TestEvent : BaseEvent(id = UUID.randomUUID())

    private class TestEventHandler(
        private val onHandle: (TestEvent) -> Unit = {},
        private val onDeadLetter: (TestEvent, Throwable) -> Unit = { _, _ -> }
    ) : EventHandler<TestEvent> {

        val deadLetterCalls = CopyOnWriteArrayList<Pair<TestEvent, Throwable>>()

        override fun supportedEventType(): Class<TestEvent> = TestEvent::class.java

        override suspend fun handle(event: TestEvent) = onHandle(event)

        override suspend fun handleDeadLetter(event: TestEvent, error: Throwable) {
            deadLetterCalls += event to error
            onDeadLetter(event, error)
        }
    }

    private class RecordingRepository(
        private val failureStatus: (Event) -> EventStatus = { EventStatus.FAILED }
    ) : EventRepository {

        val processed = CopyOnWriteArrayList<Event>()
        val failed = CopyOnWriteArrayList<Event>()
        val deadLettered = CopyOnWriteArrayList<Event>()

        override suspend fun <E : Event> fetchBatch(eventType: Class<E>): List<E> = emptyList()

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
