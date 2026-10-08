package com.fnasibov.transactional.inbox.outbox.core.domain

import com.fnasibov.transactional.inbox.outbox.core.api.BatchEventHandler
import com.fnasibov.transactional.inbox.outbox.core.api.model.BatchEventOutcome
import com.fnasibov.transactional.inbox.outbox.core.api.model.BatchResult
import com.fnasibov.transactional.inbox.outbox.core.api.model.Event
import com.fnasibov.transactional.inbox.outbox.core.api.model.EventStatus
import com.fnasibov.transactional.inbox.outbox.core.domain.exception.BatchResultValidationException
import com.fnasibov.transactional.inbox.outbox.core.domain.exception.BatchRetryRequestedException
import com.fnasibov.transactional.inbox.outbox.core.domain.exception.HandlerNotFoundException
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import java.time.Duration
import java.time.Instant
import java.util.UUID
import kotlin.coroutines.cancellation.CancellationException

/**
 * Concurrent worker that consumes whole fetched batches from a channel and executes
 * registered [BatchEventHandler]s.
 *
 * Batch boundaries produced by the fetch layer (for example a `@BatchKey` group) are
 * preserved: every consumed batch is passed to [BatchEventHandler.handleBatch] in a
 * single invocation and its per-event outcomes are applied afterwards.
 *
 * Processing flow:
 * - consume a batch from the channel
 * - resolve batch handlers for the event type
 * - execute the handlers one by one, validating each handler's outcome map against the
 *   batch before merging it into the combined result
 * - validate that every batch event received exactly one outcome in the combined result
 * - apply the outcomes: `PROCESSED` events are marked processed, `RETRY` events follow
 *   the standard failure lifecycle
 *
 * Error handling strategy:
 * - CancellationException is propagated to respect coroutine lifecycle
 * - HandlerNotFoundException → the batch is moved to DEAD_LETTER
 * - Any other exception from the handlers, and any invalid outcome map → the whole
 *   batch is marked as FAILED (with retry handling)
 *
 * If retry limit is exceeded, repository may move events to DEAD_LETTER; the handler's
 * dead-letter callback receives exactly those events with the failure that caused or
 * justified their retry.
 */
class BatchEventWorker(
    private val batchHandlers: Map<Class<out Event>, List<BatchEventHandler<out Event>>>,
    private val repository: EventRepository,
    private val concurrency: Int,
    private val channel: Channel<List<Event>>,
    private val scope: CoroutineScope,
    private val metrics: EventProcessingMetrics?
) {

    private val log = KotlinLogging.logger {}

    /**
     * Starts worker coroutines based on configured concurrency level.
     *
     * Each worker runs in an infinite loop consuming batches from its assigned channel
     * and processing them sequentially.
     *
     * Number of concurrent workers is defined by configuration.
     */
    fun start(): List<Job> =
        List(concurrency) {
            scope.launch {
                for (batch in channel) {
                    process(batch)
                }
            }
        }

    private suspend fun process(batch: List<Event>) {
        val startedAt = Instant.now()

        val handlers: List<BatchEventHandler<out Event>> = try {
            dispatch(batch)
        } catch (e: HandlerNotFoundException) {
            log.error(e) { e.message }

            batch.forEach { event ->
                if (markAsDeadLetter(event)) {
                    metrics?.recordDeadLetter()
                }
            }

            handleDeadLetterSafely(batch, emptyList(), e)
            return
        }

        val result: BatchResult = try {
            var combined = BatchResult.of(emptyMap())
            for (handler in handlers) {
                val handlerResult = invokeHandler(handler, batch)
                // Every handler must account for the whole batch on its own; otherwise a
                // partial result could be masked by another handler's complete result.
                validateResult(handlerResult, batch)
                combined = combined.mergedWith(handlerResult)
            }
            validateResult(combined, batch)
            combined
        } catch (e: CancellationException) {
            throw e
        } catch (e: Throwable) {
            log.error(e) {
                "Error while processing batch of ${batch.size} " +
                    "${batch.firstOrNull()?.javaClass?.simpleName} event(s)"
            }
            failWholeBatch(batch, handlers, e)
            return
        }

        applyOutcomes(batch, handlers, result, startedAt)
    }

    /**
     * Resolves batch handlers for the event type of the given batch.
     *
     * @throws HandlerNotFoundException if the batch is empty or no batch handlers are
     * registered for its event class
     */
    private fun dispatch(batch: List<Event>): List<BatchEventHandler<out Event>> {
        val eventType = batch.firstOrNull()?.javaClass
            ?: throw HandlerNotFoundException("Cannot dispatch an empty batch")

        return batchHandlers[eventType]
            ?: throw HandlerNotFoundException(
                "No batch handler registered for ${eventType.simpleName}"
            )
    }

    @Suppress("UNCHECKED_CAST")
    private suspend fun invokeHandler(
        handler: BatchEventHandler<out Event>,
        batch: List<Event>
    ): BatchResult =
        (handler as BatchEventHandler<Event>).handleBatch(batch)

    /**
     * Ensures the handler outcome map decides on exactly the events of the batch:
     * every batch event id must be present and no id outside the batch may appear.
     *
     * @throws BatchResultValidationException when the result does not match the batch
     */
    private fun validateResult(result: BatchResult, batch: List<Event>) {
        val batchIds = batch.map { event ->
            event.id ?: throw BatchResultValidationException(
                "Cannot apply batch outcomes: ${event.javaClass.simpleName} event has a null id"
            )
        }
        val outcomeIds = result.outcomes.keys

        val missing = batchIds.filterNot { it in outcomeIds }
        val unknown = outcomeIds.filterNot { it in batchIds.toSet() }
        if (missing.isNotEmpty() || unknown.isNotEmpty()) {
            throw BatchResultValidationException(
                "Batch outcome map does not match the processed batch: " +
                    "missing outcome for event id(s) $missing, " +
                    "unknown outcome id(s) $unknown"
            )
        }
    }

    /**
     * Applies validated per-event outcomes.
     *
     * `PROCESSED` events are marked processed and excluded from later retries. `RETRY`
     * events follow the standard failure lifecycle; when their retry limit is exhausted,
     * the handler dead-letter callback receives exactly those events with a
     * [BatchRetryRequestedException] failure.
     */
    private suspend fun applyOutcomes(
        batch: List<Event>,
        handlers: List<BatchEventHandler<out Event>>,
        result: BatchResult,
        startedAt: Instant
    ) {
        val retryRequested = mutableListOf<Event>()

        for (event in batch) {
            when (result.outcomes[event.id]) {
                BatchEventOutcome.PROCESSED -> {
                    try {
                        repository.markAsProcessed(event)
                        metrics?.recordProcessed(Duration.between(startedAt, Instant.now()))
                    } catch (e: CancellationException) {
                        throw e
                    } catch (e: Throwable) {
                        logError(e, "Failed to mark ${event.javaClass.simpleName} as processed")
                        if (markAsFailed(event)) {
                            handleDeadLetterSafely(listOf(event), handlers, e)
                        }
                    }
                }

                BatchEventOutcome.RETRY -> retryRequested += event

                // Unreachable for a validated result; retry to stay on the safe side.
                null -> retryRequested += event
            }
        }

        if (retryRequested.isNotEmpty()) {
            val error = BatchRetryRequestedException(
                "Batch handler requested retry for ${retryRequested.size} event(s)"
            )
            val deadLettered = retryRequested.filter { markAsFailed(it) }
            if (deadLettered.isNotEmpty()) {
                handleDeadLetterSafely(deadLettered, handlers, error)
            }
        }
    }

    /**
     * Applies the standard failure lifecycle to every event of the batch: all events
     * are marked as failed so that none of them is left stuck in processing, and the
     * handler dead-letter callback receives the events whose retry limit is exhausted.
     */
    private suspend fun failWholeBatch(
        batch: List<Event>,
        handlers: List<BatchEventHandler<out Event>>,
        error: Throwable
    ) {
        val deadLettered = batch.filter { markAsFailed(it) }
        if (deadLettered.isNotEmpty()) {
            handleDeadLetterSafely(deadLettered, handlers, error)
        }
    }

    /**
     * Moves the event to the dead-letter state, containing repository errors.
     *
     * @return `true` when the transition was persisted
     */
    private suspend fun markAsDeadLetter(event: Event): Boolean =
        try {
            repository.markAsDeadLetter(event)
            true
        } catch (e: CancellationException) {
            throw e
        } catch (e: Throwable) {
            logError(e, "Failed to move ${event.javaClass.simpleName} to dead letter")
            false
        }

    /**
     * Registers a processing failure for a single event.
     *
     * @return `true` when the event moved to the dead-letter state
     */
    private suspend fun markAsFailed(event: Event): Boolean =
        try {
            val status = repository.markAsFailed(event)
            metrics?.recordFailed()
            if (status == EventStatus.DEAD_LETTER) {
                metrics?.recordDeadLetter()
                true
            } else {
                false
            }
        } catch (e: CancellationException) {
            throw e
        } catch (e: Throwable) {
            logError(e, "Failed to record processing failure for ${event.javaClass.simpleName}")
            false
        }

    @Suppress("UNCHECKED_CAST")
    private suspend fun handleDeadLetterSafely(
        events: List<Event>,
        handlers: List<BatchEventHandler<out Event>>,
        error: Throwable
    ) {
        handlers.forEach { handler ->
            try {
                (handler as BatchEventHandler<Event>).handleDeadLetter(events, error)
            } catch (deadLetterError: CancellationException) {
                throw deadLetterError
            } catch (deadLetterError: Throwable) {
                log.error(deadLetterError) {
                    "Dead-letter handler failed for ${events.firstOrNull()?.javaClass?.simpleName} batch"
                }
            }
        }
    }

    private fun logError(error: Throwable, message: String) {
        log.error(error) { message }
    }
}
