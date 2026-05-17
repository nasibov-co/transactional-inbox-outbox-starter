package com.fnasibov.transactional.inbox.outbox.starter.r2dbc.domain

import com.fnasibov.transactional.inbox.outbox.starter.r2dbc.api.model.Event
import com.fnasibov.transactional.inbox.outbox.starter.r2dbc.api.model.EventStatus

/**
 * Repository abstraction for transactional event storage and processing lifecycle management.
 *
 * Provides operations for:
 * - fetching event batches for processing
 * - updating processing states
 * - handling retry and dead-letter flows
 *
 * Implementations are expected to provide concurrency-safe batch polling
 * suitable for distributed event processing environments.
 */
interface EventRepository {

    /**
     * Fetches a batch of events eligible for processing.
     *
     * Implementations may apply locking, retry backoff,
     * status filtering, and ordering policies.
     *
     * @param eventType event class to fetch
     * @return list of events ready for processing
     */
    suspend fun <E : Event> fetchBatch(eventType: Class<E>): List<E>

    /**
     * Marks the event as successfully processed.
     *
     * @param event processed event
     */
    suspend fun <E : Event> markAsProcessed(event: E)

    /**
     * Moves the event to dead-letter state.
     *
     * @param event failed event
     */
    suspend fun <E : Event> markAsDeadLetter(event: E)

    /**
     * Registers a processing failure for the event.
     *
     * Implementations may increment retry counters and decide whether
     * to keep the event for retry or move it to dead-letter state
     * depending on retry policy.
     *
     * @param event event that failed during processing
     * @return resulting status after failure handling
     */
    suspend fun <E : Event> markAsFailed(event: E): EventStatus
}