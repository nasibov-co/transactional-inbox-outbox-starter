package com.fnasibov.transactional.inbox.outbox.starter.r2dbc.configuration

import com.fnasibov.transactional.inbox.outbox.starter.r2dbc.api.EventHandler
import com.fnasibov.transactional.inbox.outbox.starter.r2dbc.api.model.Event
import com.fnasibov.transactional.inbox.outbox.starter.r2dbc.domain.BaseEventRepository
import com.fnasibov.transactional.inbox.outbox.starter.r2dbc.domain.EventProcessor
import com.fnasibov.transactional.inbox.outbox.starter.r2dbc.domain.EventProcessorStarter
import com.fnasibov.transactional.inbox.outbox.starter.r2dbc.domain.EventRepository
import com.fnasibov.transactional.inbox.outbox.starter.r2dbc.api.FetchBatchStrategy
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.transaction.ReactiveTransactionManager
import org.springframework.transaction.reactive.TransactionalOperator

@AutoConfiguration
@ConditionalOnProperty("transactional.enabled", havingValue = "true", matchIfMissing = false)
class TransactionalInboxOutboxAutoconfiguration(
    private val handlers: List<EventHandler<out Event>>
) {

    @Bean
    @ConfigurationProperties(prefix = "transactional")
    fun transactionalProperty(): TransactionalProperties {
        return TransactionalProperties()
    }

    @Bean
    fun transactionalCoroutineScope(): CoroutineScope {
        return CoroutineScope(
            SupervisorJob() + Dispatchers.IO
        )
    }

    @Bean
    fun eventRepository(
        template: R2dbcEntityTemplate,
        reactiveTransactionManager: ReactiveTransactionManager,
        properties: TransactionalProperties,
        strategies: List<FetchBatchStrategy<out Event>>
    ): BaseEventRepository {
        val strategiesByEventType = strategies.associateBy { it.eventType }
        val transactionalOperator = TransactionalOperator.create(reactiveTransactionManager)
        return BaseEventRepository(template, properties, transactionalOperator, strategiesByEventType)
    }

    @Bean
    @ConditionalOnMissingBean
    fun eventProcessor(
        transactionalProperties: TransactionalProperties,
        repository: EventRepository,
        @Qualifier("transactionalCoroutineScope")
        transactionalCoroutineScope: CoroutineScope
    ): EventProcessor {
        val handlerMap = handlers.groupBy { handler ->
            handler.supportedEventType()
        }

        return EventProcessor(handlerMap, repository, transactionalProperties, transactionalCoroutineScope)
    }

    @Bean
    fun eventProcessorStarter(
        processor: EventProcessor,
        @Qualifier("transactionalCoroutineScope")
        transactionalCoroutineScope: CoroutineScope
    ): EventProcessorStarter {
        return EventProcessorStarter(processor, transactionalCoroutineScope)
    }
}
