package com.fnasibov.transactional.inbox.outbox.autoconfigure

import com.fnasibov.transactional.inbox.outbox.core.api.BlockingFetchBatchStrategy
import com.fnasibov.transactional.inbox.outbox.core.api.FetchBatchStrategy
import com.fnasibov.transactional.inbox.outbox.core.api.asSuspendingFetchBatchStrategy
import com.fnasibov.transactional.inbox.outbox.core.api.model.Event
import com.fnasibov.transactional.inbox.outbox.core.configuration.TransactionalProperties
import com.fnasibov.transactional.inbox.outbox.core.domain.EventRepository
import com.fnasibov.transactional.inbox.outbox.r2dbc.R2dbcEventRepository
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.transaction.ReactiveTransactionManager
import org.springframework.transaction.reactive.TransactionalOperator

@AutoConfiguration(
    after = [TransactionalInboxOutboxInfrastructureAutoConfiguration::class],
    afterName = [
        "org.springframework.boot.r2dbc.autoconfigure.R2dbcAutoConfiguration",
        "org.springframework.boot.r2dbc.autoconfigure.R2dbcTransactionManagerAutoConfiguration",
        "org.springframework.boot.data.r2dbc.autoconfigure.DataR2dbcAutoConfiguration"
    ],
    before = [TransactionalInboxOutboxProcessorAutoConfiguration::class]
)
@ConditionalOnProperty("transactional.enabled", havingValue = "true")
@ConditionalOnClass(R2dbcEntityTemplate::class, R2dbcEventRepository::class)
@ConditionalOnBean(R2dbcEntityTemplate::class, ReactiveTransactionManager::class)
class TransactionalInboxOutboxR2dbcAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(EventRepository::class)
    fun r2dbcEventRepository(
        template: R2dbcEntityTemplate,
        reactiveTransactionManager: ReactiveTransactionManager,
        properties: TransactionalProperties,
        strategies: List<FetchBatchStrategy<out Event>>,
        blockingStrategies: List<BlockingFetchBatchStrategy<out Event>>
    ): EventRepository {
        val allStrategies = strategies + blockingStrategies.map { strategy ->
            @Suppress("UNCHECKED_CAST")
            (strategy as BlockingFetchBatchStrategy<Event>).asSuspendingFetchBatchStrategy()
        }
        return R2dbcEventRepository(
            template = template,
            properties = properties,
            transactionalOperator = TransactionalOperator.create(reactiveTransactionManager),
            strategiesByEventType = allStrategies.associateBy { it.eventType }
        )
    }
}
