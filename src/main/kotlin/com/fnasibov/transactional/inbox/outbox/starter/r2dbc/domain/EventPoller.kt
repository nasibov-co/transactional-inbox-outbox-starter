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

class EventPoller(
    private val eventType: Class<out Event>,
    private val repository: EventRepository,
    private val channel: Channel<Event>,
    private val properties: TransactionalProperties,
    private val scope: CoroutineScope
) {

    private val log = KotlinLogging.logger {}

    fun start() {
        scope.launch {
            var currentDelay =
                properties.polling.activeIntervalMs
            while (isActive) {
                try {
                    val batch =
                        repository.fetchBatch(eventType)
                    if (batch.isEmpty()) {
                        delay(currentDelay.toMillis())
                        currentDelay = nextDelay(
                            currentDelay,
                            properties.polling.maxIdleIntervalMs
                        )
                        continue
                    }
                    currentDelay =
                        properties.polling.activeIntervalMs
                    batch.forEach {
                        channel.send(it)
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

    private fun nextDelay(
        current: Duration,
        max: Duration
    ): Duration {
        val next = current.multipliedBy(2)
        return if (next > max) max else next
    }
}