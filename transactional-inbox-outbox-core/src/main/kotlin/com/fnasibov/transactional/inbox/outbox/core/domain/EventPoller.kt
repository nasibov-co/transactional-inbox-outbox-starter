package com.fnasibov.transactional.inbox.outbox.core.domain

import com.fnasibov.transactional.inbox.outbox.core.api.model.Event
import com.fnasibov.transactional.inbox.outbox.core.configuration.TransactionalProperties
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlin.coroutines.cancellation.CancellationException
import java.time.Duration
import kotlin.time.Duration.Companion.milliseconds

/**
 * Poller responsible for continuously fetching events of a specific type
 * from the repository and handing them to the processing pipeline.
 *
 * The poller implements adaptive polling behavior:
 * - fast polling when events are available
 * - exponential backoff when no events are found
 * - recovery delay handling on failures
 *
 * Each poller runs in its own coroutine and operates independently per event type.
 * This allows horizontal scalability and isolation between event streams.
 *
 * Every fetched batch is handed to [deliver], which forwards it downstream. The
 * delivery policy preserves the batch boundaries produced by the fetch layer: batch
 * handled event types are delivered as one unit, while individually handled events are
 * sent one by one to the processing channel.
 */
class EventPoller(
    private val eventType: Class<out Event>,
    private val repository: EventRepository,
    private val deliver: suspend (List<Event>) -> Unit,
    private val properties: TransactionalProperties,
    private val scope: CoroutineScope,
    private val metrics: EventProcessingMetrics?
) {

    private val log = KotlinLogging.logger {}

    /**
     * Starts the polling loop in a dedicated coroutine.
     *
     * The loop runs until the coroutine scope is cancelled.
     * It continuously fetches batches of events and delivers them
     * to the processing pipeline via [deliver].
     *
     * Backoff strategy:
     * - uses `activeInterval` when events are being processed
     * - doubles delay when no events are found or errors occur
     * - caps delay at `maxIdleInterval`
     */
    fun start(): Job =
        scope.launch {

            var currentDelay = properties.polling.activeInterval

            while (isActive) {
                try {
                    val batch = repository.fetchBatch(eventType)
                    metrics?.recordFetched(batch.size)

                    if (batch.isEmpty()) {
                        delay(currentDelay.toMillis().milliseconds)
                        currentDelay = nextDelay(
                            currentDelay,
                            properties.polling.maxIdleInterval
                        )
                        continue
                    }

                    currentDelay = properties.polling.activeInterval

                    deliver(batch)

                } catch (e: CancellationException) {
                    throw e

                } catch (e: Exception) {
                    log.error(e) {
                        "Polling failed for ${eventType.simpleName}"
                    }

                    delay(currentDelay.toMillis().milliseconds)

                    currentDelay = nextDelay(
                        currentDelay,
                        properties.polling.maxIdleInterval
                    )
                }
            }
        }

    /**
     * Calculates next polling delay using exponential backoff.
     *
     * @param current current delay value
     * @param max maximum allowed delay
     * @return next delay value bounded by [max]
     */
    private fun nextDelay(
        current: Duration,
        max: Duration
    ): Duration {
        val next = current.multipliedBy(2)
        return if (next > max) max else next
    }
}
