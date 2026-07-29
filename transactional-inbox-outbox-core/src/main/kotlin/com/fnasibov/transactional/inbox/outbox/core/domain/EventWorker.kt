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

                        repository.markAsDeadLetter(event)
                        metrics?.recordDeadLetter()

                        handleDeadLetterSafely(event, handlers, e)

                    } catch (e: Throwable) {
                        log.error(e) {
                            "Error while processing ${event.javaClass.simpleName}"
                        }

                        val status = repository.markAsFailed(event)
                        metrics?.recordFailed()

                        if (status == EventStatus.DEAD_LETTER) {
                            metrics?.recordDeadLetter()
                            handleDeadLetterSafely(event, handlers, e)
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
