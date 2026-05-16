package com.fnasibov.transactional.inbox.outbox.starter.r2dbc.domain

import com.fnasibov.transactional.inbox.outbox.starter.r2dbc.api.EventHandler
import com.fnasibov.transactional.inbox.outbox.starter.r2dbc.api.model.Event
import com.fnasibov.transactional.inbox.outbox.starter.r2dbc.configuration.TransactionalProperties
import com.fnasibov.transactional.inbox.outbox.starter.r2dbc.domain.exception.HandlerNotFoundException
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import kotlin.coroutines.cancellation.CancellationException

class EventWorker(
    private val handlers: Map<Class<out Event>, List<EventHandler<out Event>>>,
    private val repository: EventRepository,
    private val properties: TransactionalProperties,
    private val channel: Channel<Event>,
    private val scope: CoroutineScope
) {

    private val log = KotlinLogging.logger {}

    fun start() {
        repeat(properties.processing.concurrency) {
            scope.launch {
                for (event in channel) {
                    try {
                        dispatch(event)
                        repository.markAsProcessed(event)
                    } catch (e: CancellationException) {
                        throw e
                    } catch (e: HandlerNotFoundException) {
                        log.error(e) { e.message }
                        repository.markAsDeadLetter(event)
                    } catch (e: Throwable) {
                        log.error(e) {
                            "Error handling ${event.javaClass.simpleName}"
                        }
                        repository.markAsFailed(event)
                    }
                }
            }
        }
    }

    @Suppress("UNCHECKED_CAST")
    private suspend fun dispatch(
        event: Event
    ) {
        val handlers =
            handlers[event.javaClass]
                ?: throw HandlerNotFoundException(
                    "No handler for ${event.javaClass.simpleName}"
                )
        handlers.forEach {
            (it as EventHandler<Event>).handle(event)
        }
    }
}