package com.fnasibov.transactional.inbox.outbox.starter.r2dbc.api

import com.fnasibov.transactional.inbox.outbox.starter.r2dbc.api.model.Event

/**
 * Strategy interface for customizing batch fetching logic
 * for a specific event type.
 *
 * By default, the starter uses the built-in polling implementation
 * provided by `BaseEventRepository`. Registering a custom strategy
 * allows overriding batch selection and locking behavior
 * for a particular event class.
 *
 * Typical use cases include:
 * - custom filtering conditions
 * - priority-based polling
 * - partitioned processing
 * - database-specific optimizations
 * - custom locking strategies
 *
 * @param E event type supported by this strategy
 */
interface FetchBatchStrategy<E : Event> {

    /**
     * Event type supported by this strategy.
     */
    val eventType: Class<E>

    /**
     * Fetches a batch of events ready for processing.
     *
     * Implementations are responsible for applying any required:
     * - locking semantics
     * - transactional guarantees
     * - status transitions
     * - filtering conditions
     *
     * @return list of events selected for processing
     */
    suspend fun fetchBatch(): List<E>
}