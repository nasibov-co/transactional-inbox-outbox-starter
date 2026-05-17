package com.fnasibov.transactional.inbox.outbox.starter.r2dbc.domain

import com.fnasibov.transactional.inbox.outbox.starter.r2dbc.api.EventHandler
import com.fnasibov.transactional.inbox.outbox.starter.r2dbc.api.model.Event
import com.fnasibov.transactional.inbox.outbox.starter.r2dbc.api.model.EventStatus
import com.fnasibov.transactional.inbox.outbox.starter.r2dbc.configuration.TransactionalProperties
import com.fnasibov.transactional.inbox.outbox.starter.r2dbc.domain.exception.HandlerNotFoundException
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
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
    private val properties: TransactionalProperties,
    private val channel: Channel<Event>,
    private val scope: CoroutineScope
) {

    private val log = KotlinLogging.logger {}

    /**
     * Starts worker coroutines based on configured concurrency level.
     *
     * Each worker runs in an infinite loop consuming events from the shared channel
     * and processing them sequentially.
     *
     * Number of concurrent workers is defined by configuration.
     */
    fun start() {
        repeat(properties.processing.concurrency) {
            scope.launch {
                for (event in channel) {
                    var handlers = emptyList<EventHandler<out Event>>()
                    try {
                        handlers = dispatch(event)

                        handlers.forEach {
                            (it as EventHandler<Event>).handle(event)
                        }

                        repository.markAsProcessed(event)

                    } catch (e: CancellationException) {
                        throw e

                    } catch (e: HandlerNotFoundException) {
                        log.error(e) { e.message }

                        repository.markAsDeadLetter(event)

                        handlers.forEach {
                            (it as EventHandler<Event>).handleDeadLetter(event, e)
                        }

                    } catch (e: Throwable) {
                        log.error(e) {
                            "Error while processing ${event.javaClass.simpleName}"
                        }

                        val status = repository.markAsFailed(event)

                        if (status == EventStatus.DEAD_LETTER) {
                            handlers.forEach {
                                (it as EventHandler<Event>).handleDeadLetter(event, e)
                            }
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
}