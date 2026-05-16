package com.fnasibov.transactional.inbox.outbox.starter.r2dbc.domain

import com.fnasibov.transactional.inbox.outbox.starter.r2dbc.api.model.Event

interface EventRepository {
    suspend fun <E: Event> fetchBatch(eventType: Class<E>): List<E>
    suspend fun <E: Event> save(event: E): E
    suspend fun <E: Event> markAsProcessed(event: E)
    suspend fun <E: Event> markAsDeadLetter(event: E)
    suspend fun <E: Event> markAsFailed(event: E)
}