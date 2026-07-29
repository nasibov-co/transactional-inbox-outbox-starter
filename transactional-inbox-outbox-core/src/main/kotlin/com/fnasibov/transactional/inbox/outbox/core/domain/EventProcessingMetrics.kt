package com.fnasibov.transactional.inbox.outbox.core.domain

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import java.time.Duration

class EventProcessingMetrics(
    meterRegistry: MeterRegistry
) {

    private val fetchedCounter = Counter.builder("transactional.events.fetched")
        .description("Number of events fetched for processing")
        .register(meterRegistry)

    private val processedCounter = Counter.builder("transactional.events.processed")
        .description("Number of events processed successfully")
        .register(meterRegistry)

    private val failedCounter = Counter.builder("transactional.events.failed")
        .description("Number of event processing failures")
        .register(meterRegistry)

    private val deadLetterCounter = Counter.builder("transactional.events.dead_letter")
        .description("Number of events moved to dead letter state")
        .register(meterRegistry)

    private val processingTimer = Timer.builder("transactional.events.processing.duration")
        .description("Event processing duration")
        .register(meterRegistry)

    fun recordFetched(count: Int) {
        if (count > 0) {
            fetchedCounter.increment(count.toDouble())
        }
    }

    fun recordProcessed(duration: Duration) {
        processedCounter.increment()
        processingTimer.record(duration)
    }

    fun recordFailed() {
        failedCounter.increment()
    }

    fun recordDeadLetter() {
        deadLetterCounter.increment()
    }
}
