package com.fnasibov.transactional.inbox.outbox.starter.r2dbc.domain

import com.fnasibov.transactional.inbox.outbox.starter.r2dbc.api.model.Event
import com.fnasibov.transactional.inbox.outbox.starter.r2dbc.configuration.TransactionalProperties
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import java.time.Duration

/**
 * Poller responsible for continuously fetching events of a specific type
 * from the repository and pushing them into a processing channel.
 *
 * The poller implements adaptive polling behavior:
 * - fast polling when events are available
 * - exponential backoff when no events are found
 * - recovery delay handling on failures
 *
 * Each poller runs in its own coroutine and operates independently per event type.
 * This allows horizontal scalability and isolation between event streams.
 *
 * The fetched events are sent to a shared [Channel] for downstream processing.
 */
class EventPoller(
    private val eventType: Class<out Event>,
    private val repository: EventRepository,
    private val channel: Channel<Event>,
    private val properties: TransactionalProperties,
    private val scope: CoroutineScope
) {

    private val log = KotlinLogging.logger {}

    /**
     * Starts the polling loop in a dedicated coroutine.
     *
     * The loop runs until the coroutine scope is cancelled.
     * It continuously fetches batches of events and sends them
     * to the processing channel.
     *
     * Backoff strategy:
     * - uses `activeIntervalMs` when events are being processed
     * - doubles delay when no events are found or errors occur
     * - caps delay at `maxIdleIntervalMs`
     */
    fun start() {
        scope.launch {

            var currentDelay = properties.polling.activeIntervalMs

            while (isActive) {
                try {
                    val batch = repository.fetchBatch(eventType)

                    if (batch.isEmpty()) {
                        delay(currentDelay.toMillis())
                        currentDelay = nextDelay(
                            currentDelay,
                            properties.polling.maxIdleIntervalMs
                        )
                        continue
                    }

                    currentDelay = properties.polling.activeIntervalMs

                    batch.forEach { event ->
                        channel.send(event)
                    }

                } catch (e: Exception) {
                    log.error(e) {
                        "Polling failed for ${eventType.simpleName}"
                    }

                    delay(currentDelay.toMillis())

                    currentDelay = nextDelay(
                        currentDelay,
                        properties.polling.maxIdleIntervalMs
                    )
                }
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