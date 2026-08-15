package com.fnasibov.transactional.inbox.outbox.demo.jdbc

import com.fnasibov.transactional.inbox.outbox.core.api.BlockingEventHandler
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

@Component
class JdbcDemoEventHandler : BlockingEventHandler<JdbcDemoEvent> {

    private val logger = LoggerFactory.getLogger(javaClass)

    override fun supportedEventType(): Class<JdbcDemoEvent> =
        JdbcDemoEvent::class.java

    override fun handle(event: JdbcDemoEvent) {
        logger.info(
            "Handled JDBC demo event id={} payload={}",
            event.id,
            event.payload
        )
    }

    override fun handleDeadLetter(event: JdbcDemoEvent, error: Throwable) {
        logger.error("JDBC demo event moved to dead letter id={}", event.id, error)
    }
}
