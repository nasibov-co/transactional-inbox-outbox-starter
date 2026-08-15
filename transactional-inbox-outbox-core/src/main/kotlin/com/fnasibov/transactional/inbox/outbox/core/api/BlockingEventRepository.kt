package com.fnasibov.transactional.inbox.outbox.core.api

import com.fnasibov.transactional.inbox.outbox.core.api.model.Event
import com.fnasibov.transactional.inbox.outbox.core.api.model.EventStatus

/** Blocking alternative for clients that provide their own event repository. */
interface BlockingEventRepository {

    fun <E : Event> fetchBatch(eventType: Class<E>): List<E>

    fun <E : Event> markAsProcessed(event: E)

    fun <E : Event> markAsDeadLetter(event: E)

    fun <E : Event> markAsFailed(event: E): EventStatus
}
