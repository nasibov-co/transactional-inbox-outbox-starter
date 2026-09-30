package com.fnasibov.transactional.inbox.outbox.core.api.model

/**
 * Per-event decision reported by a batch handler for one event of the batch.
 *
 * The outcome is applied by the processing pipeline after
 * [com.fnasibov.transactional.inbox.outbox.core.api.BatchEventHandler.handleBatch] returns:
 *
 * - [PROCESSED] marks the event as [EventStatus.PROCESSED]; it is never retried again.
 * - [RETRY] routes the event through the standard failure lifecycle, as if handling had
 *   thrown for that event: the event becomes [EventStatus.FAILED] with retry backoff and
 *   moves to [EventStatus.DEAD_LETTER] once the retry limit is exhausted.
 */
enum class BatchEventOutcome {

    /**
     * The event was handled successfully and is marked [EventStatus.PROCESSED].
     */
    PROCESSED,

    /**
     * The event must be retried later through the standard retry lifecycle.
     */
    RETRY
}
