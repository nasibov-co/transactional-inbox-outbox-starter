package com.fnasibov.transactional.inbox.outbox.core.api.model

import java.time.ZonedDateTime
import java.util.*

/**
 * Base contract for transactional inbox/outbox events.
 *
 * Implementations represent persistable event records used
 * in asynchronous message processing workflows.
 *
 * The interface defines common metadata required for:
 * - event identification
 * - payload storage
 * - processing state tracking
 * - retry handling
 * - audit timestamps
 *
 * Event implementations are typically mapped to database tables
 * using Spring Data annotations such as `@Table`.
 *
 * Applications can extend [BaseEvent] when they only need to add
 * domain-specific event fields.
 */
interface Event {

    /**
     * Unique event identifier.
     */
    val id: UUID?

    /**
     * Current processing status of the event.
     */
    val status: EventStatus

    /**
     * Event creation timestamp.
     */
    val createdAt: ZonedDateTime

    /**
     * Timestamp of the latest event update.
     */
    val updatedAt: ZonedDateTime?

    /**
     * Number of processing retry attempts.
     */
    val retryCount: Int

    /**
     * Timestamp of the latest processing attempt.
     */
    val lastAttemptAt: ZonedDateTime?

    /**
     * Timestamp when the event becomes eligible for retry.
     */
    val nextRetryAt: ZonedDateTime?
}
