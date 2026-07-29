package com.fnasibov.transactional.inbox.outbox.starter.r2dbc.configuration

import com.fnasibov.transactional.inbox.outbox.core.api.FetchBatchStrategy
import com.fnasibov.transactional.inbox.outbox.core.api.model.Event
import com.fnasibov.transactional.inbox.outbox.core.configuration.TransactionalInboxOutboxInfrastructureAutoConfiguration
import com.fnasibov.transactional.inbox.outbox.core.configuration.TransactionalInboxOutboxProcessorAutoConfiguration
import com.fnasibov.transactional.inbox.outbox.core.configuration.TransactionalProperties
import com.fnasibov.transactional.inbox.outbox.starter.r2dbc.domain.BaseEventRepository
import com.fnasibov.transactional.inbox.outbox.core.domain.EventRepository
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.transaction.ReactiveTransactionManager
import org.springframework.transaction.reactive.TransactionalOperator

@AutoConfiguration(
    after = [TransactionalInboxOutboxInfrastructureAutoConfiguration::class],
    before = [TransactionalInboxOutboxProcessorAutoConfiguration::class]
)
@ConditionalOnProperty(
    "transactional.enabled",
    havingValue = "true",
    matchIfMissing = false
)
@ConditionalOnClass(R2dbcEntityTemplate::class, ReactiveTransactionManager::class)
class TransactionalInboxOutboxRepositoryAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(EventRepository::class)
    fun eventRepository(
        template: R2dbcEntityTemplate,
        reactiveTransactionManager: ReactiveTransactionManager,
        properties: TransactionalProperties,
        strategies: List<FetchBatchStrategy<out Event>>
    ): EventRepository {
        val transactionalOperator =
            TransactionalOperator.create(reactiveTransactionManager)

        return BaseEventRepository(
            template = template,
            properties = properties,
            transactionalOperator = transactionalOperator,
            strategiesByEventType = strategies.associateBy { it.eventType }
        )
    }
}
