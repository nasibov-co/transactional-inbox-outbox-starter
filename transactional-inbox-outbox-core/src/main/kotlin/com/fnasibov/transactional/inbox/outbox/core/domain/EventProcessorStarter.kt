package com.fnasibov.transactional.inbox.outbox.core.domain

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.cancel
import kotlinx.coroutines.launch
import org.springframework.context.SmartLifecycle
import java.util.concurrent.atomic.AtomicBoolean

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

    private val started = AtomicBoolean(false)

    /**
     * Starts the event processing pipeline.
     *
     * Invokes [EventProcessor.start] and marks the lifecycle as running.
     */
    override fun start() {
        if (!started.compareAndSet(false, true)) {
            return
        }

        processor.start()
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
        processor.stop()
        scope.cancel()
        started.compareAndSet(true, false)
    }

    override fun stop(callback: Runnable) {
        scope.launch {
            try {
                processor.stopGracefully()
                scope.cancel()
                started.compareAndSet(true, false)
            } finally {
                callback.run()
            }
        }
    }

    /**
     * Indicates whether the processor is currently running.
     */
    override fun isRunning(): Boolean = started.get()

    /**
     * Indicates that the component should start automatically
     * during Spring context initialization.
     */
    override fun isAutoStartup(): Boolean = true
}
