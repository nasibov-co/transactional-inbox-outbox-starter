package com.fnasibov.transactional.inbox.outbox.demo

import com.fnasibov.transactional.inbox.outbox.core.api.EventHandler
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

@Component
class DemoEventHandler : EventHandler<DemoEvent> {

    private val logger = LoggerFactory.getLogger(javaClass)

    override fun supportedEventType(): Class<DemoEvent> =
        DemoEvent::class.java

    override suspend fun handle(event: DemoEvent) {
        if (event.retryCount < 2) {
            throw RuntimeException()
        }
        logger.info(
            "Handled demo event id={} payload={}",
            event.id,
            event.payload
        )
    }

    override suspend fun handleDeadLetter(event: DemoEvent, error: Throwable) {
        logger.error("Demo event moved to dead letter id={}", event.id, error)
    }
}
