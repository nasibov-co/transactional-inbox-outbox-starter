package com.fnasibov.transactional.inbox.outbox.starter.r2dbc.domain

import com.fnasibov.transactional.inbox.outbox.starter.r2dbc.api.EventHandler
import com.fnasibov.transactional.inbox.outbox.starter.r2dbc.api.model.Event
import com.fnasibov.transactional.inbox.outbox.starter.r2dbc.configuration.TransactionalProperties
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.channels.Channel

class EventProcessor(
    private val handlers: Map<Class<out Event>, List<EventHandler<out Event>>>,
    private val repository: EventRepository,
    private val properties: TransactionalProperties,
    private val scope: CoroutineScope
) {


    private val channel = Channel<Event>(
        capacity = properties.polling.channelCapacity,
        onBufferOverflow = BufferOverflow.SUSPEND
    )

    fun start() {
        // Start pollers
        handlers
            .map { (eventType, _) -> eventType }
            .distinct()
            .map { eventType ->
                EventPoller(
                    eventType = eventType,
                    repository = repository,
                    properties = properties,
                    channel = channel,
                    scope = scope
                )
            }
            .forEach { it.start() }

        // Start worker
        EventWorker(
            handlers = handlers,
            repository = repository,
            properties = properties,
            channel = channel,
            scope = scope
        ).start()
    }
}