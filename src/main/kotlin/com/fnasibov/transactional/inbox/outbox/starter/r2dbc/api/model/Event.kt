package com.fnasibov.transactional.inbox.outbox.starter.r2dbc.api.model

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
 */
interface Event {

    /**
     * Unique event identifier.
     */
    val id: UUID

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
}