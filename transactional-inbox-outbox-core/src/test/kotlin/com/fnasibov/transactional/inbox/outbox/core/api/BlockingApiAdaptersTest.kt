package com.fnasibov.transactional.inbox.outbox.core.api

import com.fnasibov.transactional.inbox.outbox.core.api.model.BaseEvent
import com.fnasibov.transactional.inbox.outbox.core.api.model.Event
import com.fnasibov.transactional.inbox.outbox.core.api.model.EventStatus
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import java.util.concurrent.atomic.AtomicBoolean
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

    private class TestEvent : BaseEvent()
}
