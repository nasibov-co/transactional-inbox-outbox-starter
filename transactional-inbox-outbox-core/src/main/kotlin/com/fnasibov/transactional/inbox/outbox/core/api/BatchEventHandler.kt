package com.fnasibov.transactional.inbox.outbox.core.api

import com.fnasibov.transactional.inbox.outbox.core.api.model.BatchResult
import com.fnasibov.transactional.inbox.outbox.core.api.model.Event

/**
 * Interface for handling outbox events in batches.
 *
 * Unlike [EventHandler], which is invoked once per event, implementations of this
 * interface receive a whole batch of events in a single callback. The batch contains
 * only events of the supported type and is grouped by the value of the model's
 * `@BatchKey` property, so every event in the list shares the same grouping key.
 *
 * This is useful when the target system offers a batch API (for example, publishing
 * several messages at once) or when processing events of the same group together is
 * more efficient than handling them one by one.
 *
 * Processing is driven by the per-event outcomes returned from [handleBatch]: events
 * marked processed are acknowledged and excluded from later retries, while events
 * marked for retry follow the standard retry lifecycle. When [handleBatch] throws, the
 * whole batch is retried.
 *
 * An event type must be registered either with [EventHandler] beans or with
 * [BatchEventHandler] beans, never with both.
 *
 * @param E The type of the events this handler processes. Must be a subclass of [Event].
 */
interface BatchEventHandler<E : Event> {

    /**
     * Returns the class type of the events this handler supports.
     * This is used by the dispatcher to route events to the correct handler.
     *
     * @return The class object of the supported event type.
     */
    fun supportedEventType(): Class<E>

    /**
     * Handles the given batch of events and reports a per-event outcome.
     *
     * All events in [events] are of the supported type and share the same `@BatchKey`
     * value. Implementations should contain the logic for sending the events to the
     * external system. The method is suspending and completes when the whole batch is
     * handled.
     *
     * The returned [BatchResult] must contain exactly one outcome for every event of
     * [events], keyed by event id:
     * - [com.fnasibov.transactional.inbox.outbox.core.api.model.BatchEventOutcome.PROCESSED]
     *   marks that event as processed; it is never retried again.
     * - [com.fnasibov.transactional.inbox.outbox.core.api.model.BatchEventOutcome.RETRY]
     *   routes that event through the standard retry lifecycle (retry with backoff until
     *   the retry limit is exhausted, then dead letter).
     *
     * Events already handled successfully must be reported as processed so that they are
     * excluded from later retries of the batch. A result that omits an event or contains
     * ids outside [events] is rejected: the whole batch is reported as failed and follows
     * the retry lifecycle, so no message can be left stuck in processing.
     *
     * When this method throws, every event of the batch follows the standard retry
     * lifecycle, as if each of them had been reported for retry.
     *
     * @param events The events to handle. Never empty in the built-in pipeline.
     * @return Per-event outcomes keyed by event id; must cover exactly the ids of [events].
     */
    suspend fun handleBatch(events: List<E>): BatchResult

    /**
     * Handles failures that occur while processing a batch of events.
     *
     * This method is invoked when retries are exhausted for the relevant events: either
     * because [handleBatch] threw and the retry limit was reached, or because the handler
     * reported the events for retry and the retry limit was reached. It receives exactly
     * the events that moved to the dead-letter state together with the failure that
     * caused or justified their retry.
     *
     * It can be used for:
     * - logging and monitoring
     * - custom retry logic
     * - sending events to a dead-letter queue
     * - compensating actions
     *
     * @param events events that failed processing, grouped by the same `@BatchKey` value
     * @param error exception that caused the failure
     */
    suspend fun handleDeadLetter(events: List<E>, error: Throwable)
}
