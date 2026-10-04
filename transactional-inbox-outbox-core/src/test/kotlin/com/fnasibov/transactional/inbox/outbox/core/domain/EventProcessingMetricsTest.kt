package com.fnasibov.transactional.inbox.outbox.core.domain

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals

class EventProcessingMetricsTest {

    @Test
    fun `recordFetched counts only positive batch sizes`() {
        val registry = SimpleMeterRegistry()
        val metrics = EventProcessingMetrics(registry)

        metrics.recordFetched(3)
        metrics.recordFetched(0)
        metrics.recordFetched(-2)

        assertEquals(3.0, registry.get("transactional.events.fetched").counter().count())
    }

    @Test
    fun `recordProcessed counts processed events and records their duration`() {
        val registry = SimpleMeterRegistry()
        val metrics = EventProcessingMetrics(registry)

        metrics.recordProcessed(Duration.ofMillis(15))

        assertEquals(1.0, registry.get("transactional.events.processed").counter().count())
        val timer = registry.get("transactional.events.processing.duration").timer()
        assertEquals(1L, timer.count())
        assertEquals(15.0, timer.totalTime(TimeUnit.MILLISECONDS), 0.001)
    }

    @Test
    fun `recordFailed counts failures independently`() {
        val registry = SimpleMeterRegistry()
        val metrics = EventProcessingMetrics(registry)

        metrics.recordFailed()

        assertEquals(1.0, registry.get("transactional.events.failed").counter().count())
        assertEquals(0.0, registry.get("transactional.events.dead_letter").counter().count())
    }

    @Test
    fun `recordDeadLetter counts dead letter transitions independently`() {
        val registry = SimpleMeterRegistry()
        val metrics = EventProcessingMetrics(registry)

        metrics.recordDeadLetter()

        assertEquals(1.0, registry.get("transactional.events.dead_letter").counter().count())
        assertEquals(0.0, registry.get("transactional.events.failed").counter().count())
    }
}
