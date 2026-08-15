package com.fnasibov.transactional.inbox.outbox.autoconfigure

import com.fnasibov.transactional.inbox.outbox.core.domain.EventProcessorStarter
import org.springframework.boot.health.contributor.Health
import org.springframework.boot.health.contributor.HealthIndicator

class TransactionalInboxOutboxHealthIndicator(
    private val starter: EventProcessorStarter
) : HealthIndicator {

    override fun health(): Health =
        if (starter.isRunning) {
            Health.up()
                .withDetail("processor", "running")
                .build()
        } else {
            Health.outOfService()
                .withDetail("processor", "stopped")
                .build()
        }
}
