package com.fnasibov.transactional.inbox.outbox.core.api.model

import java.util.UUID

/**
 * Per-event decisions returned by
 * [com.fnasibov.transactional.inbox.outbox.core.api.BatchEventHandler.handleBatch].
 *
 * A result is keyed by event id and must contain exactly one outcome for every event of
 * the processed batch: no id may be omitted and no id outside the batch may appear. The
 * processing pipeline validates this contract; when it is violated, the whole batch is
 * treated as failed and follows the retry lifecycle, so a malformed result can never
 * leave events stuck in [EventStatus.PROCESSING].
 *
 * Build results with the factory helpers:
 * - [of] for mixed per-event outcomes,
 * - [allProcessed] when every event of the batch succeeded,
 * - [allRetried] when every event of the batch must be retried.
 */
class BatchResult private constructor(

    /**
     * Outcomes keyed by event id.
     */
    val outcomes: Map<UUID, BatchEventOutcome>
) {

    /**
     * Returns the outcome registered for the given event, or `null` when the result
     * carries no outcome for it.
     */
    fun outcomeFor(event: Event): BatchEventOutcome? =
        event.id?.let { outcomes[it] }

    /**
     * Combines this result with [other] into a single result covering both.
     *
     * When both results decide on the same event, [BatchEventOutcome.RETRY] wins over
     * [BatchEventOutcome.PROCESSED] so that an event is never acknowledged while any
     * handler still requests a retry.
     */
    fun mergedWith(other: BatchResult): BatchResult {
        val merged = LinkedHashMap<UUID, BatchEventOutcome>(outcomes)
        for ((id, outcome) in other.outcomes) {
            val current = merged[id]
            merged[id] = if (
                current == BatchEventOutcome.RETRY || outcome == BatchEventOutcome.RETRY
            ) {
                BatchEventOutcome.RETRY
            } else {
                outcome
            }
        }
        return BatchResult(merged)
    }

    companion object {

        /**
         * Creates a result from raw [outcomes] keyed by event id.
         */
        fun of(outcomes: Map<UUID, BatchEventOutcome>): BatchResult =
            BatchResult(outcomes.toMap())

        /**
         * Creates a result marking every given [events] as successfully processed.
         *
         * @throws IllegalArgumentException if any event has a null id
         */
        fun allProcessed(events: List<Event>): BatchResult =
            of(events.associate { idOf(it) to BatchEventOutcome.PROCESSED })

        /**
         * Creates a result requesting a retry for every given [events].
         *
         * @throws IllegalArgumentException if any event has a null id
         */
        fun allRetried(events: List<Event>): BatchResult =
            of(events.associate { idOf(it) to BatchEventOutcome.RETRY })

        private fun idOf(event: Event): UUID =
            requireNotNull(event.id) {
                "Batch outcomes are keyed by event id, but " +
                    "${event.javaClass.simpleName} has a null id"
            }
    }
}
