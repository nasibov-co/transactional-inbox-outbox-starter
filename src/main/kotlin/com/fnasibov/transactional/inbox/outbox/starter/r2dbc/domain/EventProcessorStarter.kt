package com.fnasibov.transactional.inbox.outbox.starter.r2dbc.domain

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.cancel
import org.springframework.context.SmartLifecycle

class EventProcessorStarter(
    private val processor: EventProcessor,
    private val scope: CoroutineScope
) : SmartLifecycle {

    @Volatile
    private var running = false

    override fun start() {
        processor.start()
        running = true
    }

    override fun stop() {
        scope.cancel()
        running = false
    }

    override fun isRunning(): Boolean = running

    override fun isAutoStartup(): Boolean = true
}