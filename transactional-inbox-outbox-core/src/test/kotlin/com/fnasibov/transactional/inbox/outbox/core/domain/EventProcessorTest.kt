package com.fnasibov.transactional.inbox.outbox.core.domain

import com.fnasibov.transactional.inbox.outbox.core.api.EventHandler
import com.fnasibov.transactional.inbox.outbox.core.api.model.BaseEvent
import com.fnasibov.transactional.inbox.outbox.core.api.model.Event
import com.fnasibov.transactional.inbox.outbox.core.api.model.EventStatus
import com.fnasibov.transactional.inbox.outbox.core.configuration.TransactionalProperties
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertFalse

class EventProcessorTest {

    @Test
    fun `stop waits for workers to drain in-flight events`() {
        runBlocking {
            val event = TestEvent()
            val handlerStarted = CompletableDeferred<Unit>()
            val handlerCanFinish = CompletableDeferred<Unit>()
            val repository = SingleEventRepository(event)
            val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
            val processor = EventProcessor(
                handlers = mapOf(
                    TestEvent::class.java to listOf(
                        DelayingEventHandler(handlerStarted, handlerCanFinish)
                    )
                ),
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

            try {
                processor.start()
                withTimeout(1_000) {
                    handlerStarted.await()
                }

                val stop = async {
                    processor.stopGracefully()
                }

                delay(50)
                assertFalse(stop.isCompleted)

                handlerCanFinish.complete(Unit)

                withTimeout(1_000) {
                    stop.await()
                    repository.processed.await()
                }
            } finally {
                scope.cancel()
            }
        }
    }

    @Test
    fun `event types process through independent channels`() {
        runBlocking {
            val slowEvent = SlowTestEvent()
            val fastEvent = FastTestEvent()
            val slowHandlerStarted = CompletableDeferred<Unit>()
            val slowHandlerCanFinish = CompletableDeferred<Unit>()
            val fastHandlerProcessed = CompletableDeferred<Unit>()
            val repository = IndependentChannelsRepository(
                slowEvent = slowEvent,
                fastEvent = fastEvent,
                slowHandlerStarted = slowHandlerStarted
            )
            val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
            val processor = EventProcessor(
                handlers = mapOf(
                    SlowTestEvent::class.java to listOf(
                        SlowEventHandler(slowHandlerStarted, slowHandlerCanFinish)
                    ),
                    FastTestEvent::class.java to listOf(
                        FastEventHandler(fastHandlerProcessed)
                    )
                ),
                repository = repository,
                properties = TransactionalProperties(
                    polling = TransactionalProperties.Polling(
                        activeInterval = Duration.ofMillis(10)
                    ),
                    processing = TransactionalProperties.Processing(
                        concurrency = 1,
                        shutdownTimeout = Duration.ofSeconds(1)
                    )
                ),
                scope = scope,
                metrics = null
            )

            try {
                processor.start()

                withTimeout(1_000) {
                    slowHandlerStarted.await()
                    fastHandlerProcessed.await()
                }

                slowHandlerCanFinish.complete(Unit)

                withTimeout(1_000) {
                    repository.processed.await()
                }
            } finally {
                scope.cancel()
            }
        }
    }

    @Test
    fun `event type concurrency overrides default concurrency`() {
        runBlocking {
            val events = listOf(ParallelTestEvent(), ParallelTestEvent())
            val handler = ParallelEventHandler()
            val repository = BatchEventRepository(
                eventType = ParallelTestEvent::class.java,
                events = events
            )
            val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
            val processor = EventProcessor(
                handlers = mapOf(
                    ParallelTestEvent::class.java to listOf(handler)
                ),
                repository = repository,
                properties = TransactionalProperties(
                    polling = TransactionalProperties.Polling(
                        activeInterval = Duration.ofMillis(10)
                    ),
                    processing = TransactionalProperties.Processing(
                        concurrency = 1,
                        eventTypes = listOf(
                            TransactionalProperties.EventTypeProcessing(
                                eventType = ParallelTestEvent::class.java.simpleName,
                                concurrency = 2
                            )
                        ),
                        shutdownTimeout = Duration.ofSeconds(1)
                    )
                ),
                scope = scope,
                metrics = null
            )

            try {
                processor.start()

                withTimeout(1_000) {
                    handler.twoEventsStarted.await()
                }

                handler.canFinish.complete(Unit)

                withTimeout(1_000) {
                    repository.processed.await()
                }
            } finally {
                scope.cancel()
            }
        }
    }

    private class TestEvent : BaseEvent(
        id = UUID.randomUUID()
    )

    private class SlowTestEvent : BaseEvent(
        id = UUID.randomUUID()
    )

    private class FastTestEvent : BaseEvent(
        id = UUID.randomUUID()
    )

    private class ParallelTestEvent : BaseEvent(
        id = UUID.randomUUID()
    )

    private class DelayingEventHandler(
        private val started: CompletableDeferred<Unit>,
        private val canFinish: CompletableDeferred<Unit>
    ) : EventHandler<TestEvent> {

        override fun supportedEventType(): Class<TestEvent> =
            TestEvent::class.java

        override suspend fun handle(event: TestEvent) {
            started.complete(Unit)
            canFinish.await()
        }

        override suspend fun handleDeadLetter(
            event: TestEvent,
            error: Throwable
        ) = Unit
    }

    private class SingleEventRepository(
        private val event: TestEvent
    ) : EventRepository {

        private val fetched = AtomicBoolean(false)
        val processed = CompletableDeferred<TestEvent>()

        @Suppress("UNCHECKED_CAST")
        override suspend fun <E : Event> fetchBatch(eventType: Class<E>): List<E> =
            if (fetched.compareAndSet(false, true)) {
                listOf(event as E)
            } else {
                emptyList()
            }

        override suspend fun <E : Event> markAsProcessed(event: E) {
            processed.complete(event as TestEvent)
        }

        override suspend fun <E : Event> markAsDeadLetter(event: E) = Unit

        override suspend fun <E : Event> markAsFailed(event: E): EventStatus =
            EventStatus.FAILED
    }

    private class SlowEventHandler(
        private val started: CompletableDeferred<Unit>,
        private val canFinish: CompletableDeferred<Unit>
    ) : EventHandler<SlowTestEvent> {

        override fun supportedEventType(): Class<SlowTestEvent> =
            SlowTestEvent::class.java

        override suspend fun handle(event: SlowTestEvent) {
            started.complete(Unit)
            canFinish.await()
        }

        override suspend fun handleDeadLetter(
            event: SlowTestEvent,
            error: Throwable
        ) = Unit
    }

    private class FastEventHandler(
        private val processed: CompletableDeferred<Unit>
    ) : EventHandler<FastTestEvent> {

        override fun supportedEventType(): Class<FastTestEvent> =
            FastTestEvent::class.java

        override suspend fun handle(event: FastTestEvent) {
            processed.complete(Unit)
        }

        override suspend fun handleDeadLetter(
            event: FastTestEvent,
            error: Throwable
        ) = Unit
    }

    private class ParallelEventHandler : EventHandler<ParallelTestEvent> {

        private val started = AtomicInteger()
        val twoEventsStarted = CompletableDeferred<Unit>()
        val canFinish = CompletableDeferred<Unit>()

        override fun supportedEventType(): Class<ParallelTestEvent> =
            ParallelTestEvent::class.java

        override suspend fun handle(event: ParallelTestEvent) {
            if (started.incrementAndGet() == 2) {
                twoEventsStarted.complete(Unit)
            }
            canFinish.await()
        }

        override suspend fun handleDeadLetter(
            event: ParallelTestEvent,
            error: Throwable
        ) = Unit
    }

    private class IndependentChannelsRepository(
        private val slowEvent: SlowTestEvent,
        private val fastEvent: FastTestEvent,
        private val slowHandlerStarted: CompletableDeferred<Unit>
    ) : EventRepository {

        private val fetchedSlow = AtomicBoolean(false)
        private val fetchedFast = AtomicBoolean(false)
        private val processedCount = AtomicInteger()
        val processed = CompletableDeferred<Unit>()

        @Suppress("UNCHECKED_CAST")
        override suspend fun <E : Event> fetchBatch(eventType: Class<E>): List<E> =
            when (eventType) {
                SlowTestEvent::class.java ->
                    if (fetchedSlow.compareAndSet(false, true)) {
                        listOf(slowEvent as E)
                    } else {
                        emptyList()
                    }

                FastTestEvent::class.java -> {
                    slowHandlerStarted.await()
                    if (fetchedFast.compareAndSet(false, true)) {
                        listOf(fastEvent as E)
                    } else {
                        emptyList()
                    }
                }

                else -> emptyList()
            }

        override suspend fun <E : Event> markAsProcessed(event: E) {
            if (processedCount.incrementAndGet() == 2) {
                processed.complete(Unit)
            }
        }

        override suspend fun <E : Event> markAsDeadLetter(event: E) = Unit

        override suspend fun <E : Event> markAsFailed(event: E): EventStatus =
            EventStatus.FAILED
    }

    private class BatchEventRepository(
        private val eventType: Class<out Event>,
        private val events: List<Event>
    ) : EventRepository {

        private val fetched = AtomicBoolean(false)
        private val processedCount = AtomicInteger()
        val processed = CompletableDeferred<Unit>()

        @Suppress("UNCHECKED_CAST")
        override suspend fun <E : Event> fetchBatch(eventType: Class<E>): List<E> =
            if (this.eventType == eventType && fetched.compareAndSet(false, true)) {
                events as List<E>
            } else {
                emptyList()
            }

        override suspend fun <E : Event> markAsProcessed(event: E) {
            if (processedCount.incrementAndGet() == events.size) {
                processed.complete(Unit)
            }
        }

        override suspend fun <E : Event> markAsDeadLetter(event: E) = Unit

        override suspend fun <E : Event> markAsFailed(event: E): EventStatus =
            EventStatus.FAILED
    }
}
