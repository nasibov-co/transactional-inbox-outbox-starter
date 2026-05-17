package com.fnasibov.transactional.inbox.outbox.starter.r2dbc.api.model

/**
 * Processing lifecycle statuses for transactional events.
 *
 * Status transitions are managed by the event processing infrastructure
 * during polling, retry, and completion stages.
 */
enum class EventStatus {

    /**
     * Event is waiting to be processed.
     */
    PENDING,

    /**
     * Event has been locked and is currently being processed.
     */
    PROCESSING,

    /**
     * Event was successfully processed.
     */
    PROCESSED,

    /**
     * Event processing failed but can still be retried.
     */
    FAILED,

    /**
     * Event exceeded retry limits and was moved
     * to the dead-letter state.
     */
    DEAD_LETTER
}