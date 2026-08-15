package com.fnasibov.transactional.inbox.outbox.core.api

import com.fnasibov.transactional.inbox.outbox.core.api.model.Event

/**
 * Blocking alternative to [EventHandler] for clients that do not use coroutines.
 * Implementations are executed on [kotlinx.coroutines.Dispatchers.IO].
 */
interface BlockingEventHandler<E : Event> {

    fun supportedEventType(): Class<E>

    fun handle(event: E)

    fun handleDeadLetter(event: E, error: Throwable)
}
