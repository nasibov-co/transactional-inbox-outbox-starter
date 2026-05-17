package com.example.demo

import com.fnasibov.transactional.inbox.outbox.starter.r2dbc.api.EventHandler
import io.github.oshai.kotlinlogging.KotlinLogging
import org.springframework.stereotype.Component

@Component
class MyEventHandler : EventHandler<MyEvent> {
    val logger = KotlinLogging.logger {  }
    override fun supportedEventType(): Class<MyEvent> {
        return MyEvent::class.java
    }

    override suspend fun handle(event: MyEvent) {
        logger.info { event.payload }
    }

    override suspend fun handleError(event: MyEvent, error: Throwable) {
        logger.error { error.message }
    }

}