package com.fnasibov.transactional.inbox.outbox.autoconfigure

import com.fnasibov.transactional.inbox.outbox.core.api.BlockingEventHandler
import com.fnasibov.transactional.inbox.outbox.core.api.EventHandler
import com.fnasibov.transactional.inbox.outbox.core.api.asSuspendingEventHandler
import com.fnasibov.transactional.inbox.outbox.core.api.model.Event
import com.fnasibov.transactional.inbox.outbox.core.configuration.TransactionalProperties
import com.fnasibov.transactional.inbox.outbox.core.domain.EventProcessingMetrics
import com.fnasibov.transactional.inbox.outbox.core.domain.EventProcessor
import com.fnasibov.transactional.inbox.outbox.core.domain.EventRepository
import kotlinx.coroutines.CoroutineScope
import org.springframework.beans.factory.ObjectProvider
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean

@AutoConfiguration(after = [TransactionalInboxOutboxInfrastructureAutoConfiguration::class])
@ConditionalOnProperty(
    "transactional.enabled",
    havingValue = "true",
    matchIfMissing = false
)
@ConditionalOnBean(EventRepository::class)
class TransactionalInboxOutboxProcessorAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    fun eventProcessor(
        handlers: List<EventHandler<out Event>>,
        blockingHandlers: List<BlockingEventHandler<out Event>>,
        transactionalProperties: TransactionalProperties,
        repository: EventRepository,
        @Qualifier("transactionalCoroutineScope")
        transactionalCoroutineScope: CoroutineScope,
        eventProcessingMetrics: ObjectProvider<EventProcessingMetrics>
    ): EventProcessor {
        val allHandlers = handlers + blockingHandlers.map { handler ->
            @Suppress("UNCHECKED_CAST")
            (handler as BlockingEventHandler<Event>).asSuspendingEventHandler()
        }
        val handlerMap = allHandlers.groupBy { handler ->
            handler.supportedEventType()
        }

        return EventProcessor(
            handlers = handlerMap,
            repository = repository,
            properties = transactionalProperties,
            scope = transactionalCoroutineScope,
            metrics = eventProcessingMetrics.ifAvailable
        )
    }
}
