package com.fnasibov.transactional.inbox.outbox.starter.r2dbc.domain

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.cancel
import org.springframework.context.SmartLifecycle

/**
 * Spring lifecycle adapter responsible for starting and stopping
 * the transactional event processing pipeline.
 *
 * This component integrates the [EventProcessor] with Spring Boot lifecycle
 * management via [SmartLifecycle].
 *
 * Responsibilities:
 * - automatically starting the processor on application startup
 * - gracefully stopping all coroutines on shutdown
 * - managing lifecycle state tracking
 */
class EventProcessorStarter(
    private val processor: EventProcessor,
    private val scope: CoroutineScope
) : SmartLifecycle {

    @Volatile
    private var running = false

    /**
     * Starts the event processing pipeline.
     *
     * Invokes [EventProcessor.start] and marks the lifecycle as running.
     */
    override fun start() {
        processor.start()
        running = true
    }

    /**
     * Stops the event processing pipeline.
     *
     * Cancels the shared coroutine scope, which terminates:
     * - all pollers
     * - event workers
     * - background processing tasks
     */
    override fun stop() {
        scope.cancel()
        running = false
    }

    /**
     * Indicates whether the processor is currently running.
     */
    override fun isRunning(): Boolean = running

    /**
     * Indicates that the component should start automatically
     * during Spring context initialization.
     */
    override fun isAutoStartup(): Boolean = true
}