package com.fnasibov.transactional.inbox.outbox.demo

import com.fnasibov.transactional.inbox.outbox.core.api.BatchEventHandler
import com.fnasibov.transactional.inbox.outbox.core.api.model.BatchResult
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

/**
 * Demo batch handler for [BatchDemoEvent].
 *
 * Every invocation receives all events of one `@BatchKey` group at once. The handler
 * logs the shared key, the batch size, and the event ids so the grouped delivery is
 * visible in the application log, then reports every event as processed so the demo
 * does not enter a retry loop.
 */
@Component
class BatchDemoEventHandler : BatchEventHandler<BatchDemoEvent> {

    private val logger = LoggerFactory.getLogger(javaClass)

    override fun supportedEventType(): Class<BatchDemoEvent> =
        BatchDemoEvent::class.java

    override suspend fun handleBatch(events: List<BatchDemoEvent>): BatchResult {
        logger.info(
            "Handled batch of {} event(s) for batchKey={} eventIds={}",
            events.size,
            events.firstOrNull()?.batchKey,
            events.map { it.id }
        )
        return BatchResult.allProcessed(events)
    }

    override suspend fun handleDeadLetter(events: List<BatchDemoEvent>, error: Throwable) {
        logger.error(
            "Batch demo events moved to dead letter batchKey={} eventIds={}",
            events.firstOrNull()?.batchKey,
            events.map { it.id },
            error
        )
    }
}