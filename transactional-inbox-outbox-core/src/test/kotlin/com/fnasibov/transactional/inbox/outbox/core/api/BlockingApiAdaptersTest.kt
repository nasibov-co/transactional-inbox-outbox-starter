package com.fnasibov.transactional.inbox.outbox.core.api

import com.fnasibov.transactional.inbox.outbox.core.api.model.BaseEvent
import com.fnasibov.transactional.inbox.outbox.core.api.model.Event
import com.fnasibov.transactional.inbox.outbox.core.api.model.EventStatus
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.test.assertEquals
import kotlin.test.assertSame
import kotlin.test.assertTrue

class BlockingApiAdaptersTest {

    @Test
    fun `blocking handler can be used by suspending pipeline`() = runBlocking {
        val handled = AtomicBoolean()
        val handler = object : BlockingEventHandler<TestEvent> {
            override fun supportedEventType(): Class<TestEvent> = TestEvent::class.java
            override fun handle(event: TestEvent) {
                handled.set(true)
            }
            override fun handleDeadLetter(event: TestEvent, error: Throwable) = Unit
        }

        handler.asSuspendingEventHandler().handle(TestEvent())

        assertTrue(handled.get())
    }

    @Test
    fun `blocking fetch strategy can be used by suspending repository`() = runBlocking {
        val event = TestEvent()
        val strategy = object : BlockingFetchBatchStrategy<TestEvent> {
            override val eventType: Class<TestEvent> = TestEvent::class.java
            override fun fetchBatch(): List<TestEvent> = listOf(event)
        }

        val result = strategy.asSuspendingFetchBatchStrategy().fetchBatch()

        assertTrue(result.single() === event)
    }

    @Test
    fun `blocking repository can be used by suspending pipeline`() = runBlocking {
        val event = TestEvent()
        val repository = object : BlockingEventRepository {
            @Suppress("UNCHECKED_CAST")
            override fun <E : Event> fetchBatch(eventType: Class<E>): List<E> = listOf(event as E)
            override fun <E : Event> markAsProcessed(event: E) = Unit
            override fun <E : Event> markAsDeadLetter(event: E) = Unit
            override fun <E : Event> markAsFailed(event: E): EventStatus = EventStatus.FAILED
        }

        val result = repository.asSuspendingEventRepository().fetchBatch(TestEvent::class.java)

        assertTrue(result.single() === event)
    }

    @Test
    fun `blocking handler dead letter callback is delegated with the original event and error`() = runBlocking {
        val deadLetterCalls = mutableListOf<Pair<TestEvent, Throwable>>()
        val handler = object : BlockingEventHandler<TestEvent> {
            override fun supportedEventType(): Class<TestEvent> = TestEvent::class.java
            override fun handle(event: TestEvent) = Unit
            override fun handleDeadLetter(event: TestEvent, error: Throwable) {
                deadLetterCalls += event to error
            }
        }
        val event = TestEvent()
        val error = IllegalStateException("publish failed")

        handler.asSuspendingEventHandler().handleDeadLetter(event, error)

        val (deadLettered, reportedError) = deadLetterCalls.single()
        assertSame(event, deadLettered)
        assertSame(error, reportedError)
    }

    @Test
    fun `blocking repository status updates are delegated and the failure status is returned`() = runBlocking {
        val processed = mutableListOf<Event>()
        val deadLettered = mutableListOf<Event>()
        val failed = mutableListOf<Event>()
        val repository = object : BlockingEventRepository {
            override fun <E : Event> fetchBatch(eventType: Class<E>): List<E> = emptyList()
            override fun <E : Event> markAsProcessed(event: E) {
                processed += event
            }
            override fun <E : Event> markAsDeadLetter(event: E) {
                deadLettered += event
            }
            override fun <E : Event> markAsFailed(event: E): EventStatus {
                failed += event
                return EventStatus.DEAD_LETTER
            }
        }
        val suspending = repository.asSuspendingEventRepository()
        val event = TestEvent()

        suspending.markAsProcessed(event)
        suspending.markAsDeadLetter(event)
        val status = suspending.markAsFailed(event)

        assertSame(event, processed.single())
        assertSame(event, deadLettered.single())
        assertSame(event, failed.single())
        assertEquals(EventStatus.DEAD_LETTER, status)
    }

    @Test
    fun `adapters expose the supported event type of their blocking delegates`() {
        val handler = object : BlockingEventHandler<TestEvent> {
            override fun supportedEventType(): Class<TestEvent> = TestEvent::class.java
            override fun handle(event: TestEvent) = Unit
            override fun handleDeadLetter(event: TestEvent, error: Throwable) = Unit
        }
        val strategy = object : BlockingFetchBatchStrategy<TestEvent> {
            override val eventType: Class<TestEvent> = TestEvent::class.java
            override fun fetchBatch(): List<TestEvent> = emptyList()
        }

        assertSame(TestEvent::class.java, handler.asSuspendingEventHandler().supportedEventType())
        assertSame(TestEvent::class.java, strategy.asSuspendingFetchBatchStrategy().eventType)
    }

    private class TestEvent : BaseEvent()
}
