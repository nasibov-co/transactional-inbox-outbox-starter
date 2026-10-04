package com.fnasibov.transactional.inbox.outbox.core.domain

import com.fnasibov.transactional.inbox.outbox.core.api.BatchEventHandler
import com.fnasibov.transactional.inbox.outbox.core.api.EventHandler
import com.fnasibov.transactional.inbox.outbox.core.api.model.Event
import com.fnasibov.transactional.inbox.outbox.core.configuration.TransactionalProperties
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.time.Duration.Companion.milliseconds

/**
 * Core orchestrator of the transactional event processing pipeline.
 *
 * The processor is responsible for:
 * - starting event pollers per event type
 * - coordinating event dispatch through event type specific internal channels
 * - delegating execution to event handlers via a worker
 *
 * The processing model is fully asynchronous and based on coroutines.
 * Each event type is polled and processed independently through its own
 * channel and worker pipeline.
 *
 * Event types registered with [EventHandler] are processed event by event, while event
 * types registered with [BatchEventHandler] keep their fetched batch boundaries and are
 * processed as whole batches. An event type must be registered with exactly one of the
 * two handler styles; mixing them is rejected at construction time.
 *
 * Architecture overview:
 * EventPoller(s) -> Channel -> EventWorker -> EventHandler(s)
 * EventPoller(s) -> Channel -> BatchEventWorker -> BatchEventHandler(s)
 */
class EventProcessor(
    private val handlers: Map<Class<out Event>, List<EventHandler<out Event>>>,
    private val batchHandlers: Map<Class<out Event>, List<BatchEventHandler<out Event>>> = emptyMap(),
    private val repository: EventRepository,
    private val properties: TransactionalProperties,
    private val scope: CoroutineScope,
    private val metrics: EventProcessingMetrics?,
) {
    private val started = AtomicBoolean(false)
    private val pollerJobs = mutableListOf<Job>()
    private val workerJobs = mutableListOf<Job>()
    private val channels = mutableListOf<Channel<*>>()

    init {
        val conflicting = handlers.keys.intersect(batchHandlers.keys)
        require(conflicting.isEmpty()) {
            "Event type(s) ${conflicting.joinToString { it.simpleName }} are registered " +
                "with both EventHandler and BatchEventHandler; register exactly one " +
                "handler style per event type"
        }
    }

    /**
     * Starts the event processing pipeline.
     *
     * This method:
     * - initializes one [EventPoller] per event type
     * - starts an [EventWorker] group per individually handled event type
     * - starts a [BatchEventWorker] group per batch handled event type
     *
     * After invocation, the system continuously polls, buffers,
     * and processes events until the coroutine scope is cancelled.
     */
    fun start() {
        if (!started.compareAndSet(false, true)) {
            return
        }

        handlers.keys.distinct().forEach { eventType ->
            /*
             * Internal buffer used to decouple polling and processing stages.
             *
             * Capacity is configured via `transactional.polling.channel-capacity`.
             * Overflow strategy is set to SUSPEND to ensure backpressure.
             */
            val processingChannel =
                Channel<Event>(
                    capacity = properties.polling.channelCapacity,
                    onBufferOverflow = BufferOverflow.SUSPEND,
                )

            channels += processingChannel

            pollerJobs +=
                EventPoller(
                    eventType = eventType,
                    repository = repository,
                    deliver = { batch ->
                        batch.forEach { event -> processingChannel.send(event) }
                    },
                    properties = properties,
                    scope = scope,
                    metrics = metrics,
                ).start()

            workerJobs +=
                EventWorker(
                    handlers = handlers,
                    repository = repository,
                    concurrency = properties.processing.concurrencyFor(eventType),
                    channel = processingChannel,
                    scope = scope,
                    metrics = metrics,
                ).start()
        }

        batchHandlers.keys.distinct().forEach { eventType ->
            /*
             * Batch handled event types keep the batch boundaries produced by the
             * fetch layer: one channel element is one whole fetched batch.
             */
            val processingChannel =
                Channel<List<Event>>(
                    capacity = properties.polling.channelCapacity,
                    onBufferOverflow = BufferOverflow.SUSPEND,
                )

            channels += processingChannel

            pollerJobs +=
                EventPoller(
                    eventType = eventType,
                    repository = repository,
                    deliver = { batch -> processingChannel.send(batch) },
                    properties = properties,
                    scope = scope,
                    metrics = metrics,
                ).start()

            workerJobs +=
                BatchEventWorker(
                    batchHandlers = batchHandlers,
                    repository = repository,
                    concurrency = properties.processing.concurrencyFor(eventType),
                    channel = processingChannel,
                    scope = scope,
                    metrics = metrics,
                ).start()
        }
    }

    fun stop() =
        runBlocking {
            stopGracefully()
        }

    suspend fun stopGracefully() {
        if (!started.compareAndSet(true, false)) {
            return
        }

        pollerJobs.forEach { it.cancelAndJoin() }
        channels.forEach { it.close() }

        val drained =
            withTimeoutOrNull(
                properties.processing.shutdownTimeout
                    .toMillis()
                    .milliseconds,
            ) {
                workerJobs.joinAll()
            } != null

        if (!drained) {
            workerJobs.forEach { it.cancelAndJoin() }
        }

        pollerJobs.clear()
        workerJobs.clear()
        channels.clear()
    }
}
