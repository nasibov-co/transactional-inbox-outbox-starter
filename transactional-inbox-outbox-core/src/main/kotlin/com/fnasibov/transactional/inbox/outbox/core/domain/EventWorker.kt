package com.fnasibov.transactional.inbox.outbox.core.domain

import com.fnasibov.transactional.inbox.outbox.core.api.EventHandler
import com.fnasibov.transactional.inbox.outbox.core.api.model.Event
import com.fnasibov.transactional.inbox.outbox.core.api.model.EventStatus
import com.fnasibov.transactional.inbox.outbox.core.domain.exception.HandlerNotFoundException
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import java.time.Duration
import java.time.Instant
import kotlin.coroutines.cancellation.CancellationException

/**
 * Concurrent worker that consumes events from a channel and executes registered handlers.
 *
 * This class represents the execution layer of the transactional event processing pipeline.
 *
 * Processing flow:
 * - consume event from channel
 * - resolve handlers for event type
 * - execute all handlers
 * - update event status based on outcome
 *
 * Error handling strategy:
 * - CancellationException is propagated to respect coroutine lifecycle
 * - HandlerNotFoundException → event is moved to DEAD_LETTER
 * - Any other exception → event is marked as FAILED (with retry handling)
 *
 * If retry limit is exceeded, repository may move event to DEAD_LETTER.
 *
 * Repository status-write failures are logged and contained so the worker keeps
 * consuming subsequent events; success metrics and dead-letter callbacks are only
 * reported when the corresponding status transition was persisted.
 */
class EventWorker(
    private val handlers: Map<Class<out Event>, List<EventHandler<out Event>>>,
    private val repository: EventRepository,
    private val concurrency: Int,
    private val channel: Channel<Event>,
    private val scope: CoroutineScope,
    private val metrics: EventProcessingMetrics?
) {

    private val log = KotlinLogging.logger {}

    /**
     * Starts worker coroutines based on configured concurrency level.
     *
     * Each worker runs in an infinite loop consuming events from its assigned channel
     * and processing them sequentially.
     *
     * Number of concurrent workers is defined by configuration.
     */
    @Suppress("UNCHECKED_CAST")
    fun start(): List<Job> =
        List(concurrency) {
            scope.launch {
                for (event in channel) {
                    var handlers = emptyList<EventHandler<out Event>>()
                    val startedAt = Instant.now()
                    try {
                        handlers = dispatch(event)

                        handlers.forEach {
                            (it as EventHandler<Event>).handle(event)
                        }

                        repository.markAsProcessed(event)
                        metrics?.recordProcessed(Duration.between(startedAt, Instant.now()))

                    } catch (e: CancellationException) {
                        throw e

                    } catch (e: HandlerNotFoundException) {
                        log.error(e) { e.message }

                        if (markAsDeadLetter(event)) {
                            metrics?.recordDeadLetter()
                            handleDeadLetterSafely(event, handlers, e)
                        }

                    } catch (e: Throwable) {
                        log.error(e) {
                            "Error while processing ${event.javaClass.simpleName}"
                        }

                        val status = markAsFailed(event)
                        if (status != null) {
                            metrics?.recordFailed()

                            if (status == EventStatus.DEAD_LETTER) {
                                metrics?.recordDeadLetter()
                                handleDeadLetterSafely(event, handlers, e)
                            }
                        }
                    }
                }
            }
        }

    /**
     * Resolves handlers for the given event type.
     *
     * @throws HandlerNotFoundException if no handlers are registered for the event class
     */
    @Suppress("UNCHECKED_CAST")
    private fun dispatch(event: Event): List<EventHandler<out Event>> {
        return handlers[event.javaClass]
            ?: throw HandlerNotFoundException(
                "No handler registered for ${event.javaClass.simpleName}"
            )
    }

    /**
     * Moves the event to the dead-letter state, containing repository errors.
     *
     * @return `true` when the transition was persisted
     */
    private suspend fun markAsDeadLetter(event: Event): Boolean =
        try {
            repository.markAsDeadLetter(event)
            true
        } catch (e: CancellationException) {
            throw e
        } catch (e: Throwable) {
            log.error(e) {
                "Failed to move ${event.javaClass.simpleName} to dead letter"
            }
            false
        }

    /**
     * Registers a processing failure for the event, containing repository errors.
     *
     * @return the recorded status, or `null` when the failure could not be persisted
     */
    private suspend fun markAsFailed(event: Event): EventStatus? =
        try {
            repository.markAsFailed(event)
        } catch (e: CancellationException) {
            throw e
        } catch (e: Throwable) {
            log.error(e) {
                "Failed to record processing failure for ${event.javaClass.simpleName}"
            }
            null
        }

    @Suppress("UNCHECKED_CAST")
    private suspend fun handleDeadLetterSafely(
        event: Event,
        handlers: List<EventHandler<out Event>>,
        error: Throwable
    ) {
        handlers.forEach { handler ->
            try {
                (handler as EventHandler<Event>).handleDeadLetter(event, error)
            } catch (deadLetterError: CancellationException) {
                throw deadLetterError
            } catch (deadLetterError: Throwable) {
                log.error(deadLetterError) {
                    "Dead-letter handler failed for ${event.javaClass.simpleName}"
                }
            }
        }
    }
}
