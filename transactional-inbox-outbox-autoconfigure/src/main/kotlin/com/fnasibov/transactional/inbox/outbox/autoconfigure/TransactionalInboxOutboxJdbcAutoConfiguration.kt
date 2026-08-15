package com.fnasibov.transactional.inbox.outbox.autoconfigure

import com.fnasibov.transactional.inbox.outbox.core.api.BlockingFetchBatchStrategy
import com.fnasibov.transactional.inbox.outbox.core.api.FetchBatchStrategy
import com.fnasibov.transactional.inbox.outbox.core.api.asSuspendingFetchBatchStrategy
import com.fnasibov.transactional.inbox.outbox.core.api.model.Event
import com.fnasibov.transactional.inbox.outbox.core.configuration.TransactionalProperties
import com.fnasibov.transactional.inbox.outbox.core.domain.EventRepository
import com.fnasibov.transactional.inbox.outbox.jdbc.JdbcEventRepository
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean
import org.springframework.data.jdbc.core.JdbcAggregateOperations
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.support.TransactionTemplate

@AutoConfiguration(
    after = [TransactionalInboxOutboxInfrastructureAutoConfiguration::class],
    before = [TransactionalInboxOutboxProcessorAutoConfiguration::class]
)
@ConditionalOnProperty("transactional.enabled", havingValue = "true")
@ConditionalOnClass(JdbcAggregateOperations::class, JdbcEventRepository::class)
@ConditionalOnBean(
    NamedParameterJdbcOperations::class,
    JdbcAggregateOperations::class,
    PlatformTransactionManager::class
)
class TransactionalInboxOutboxJdbcAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(EventRepository::class)
    fun jdbcEventRepository(
        jdbc: NamedParameterJdbcOperations,
        aggregates: JdbcAggregateOperations,
        transactionManager: PlatformTransactionManager,
        properties: TransactionalProperties,
        strategies: List<FetchBatchStrategy<out Event>>,
        blockingStrategies: List<BlockingFetchBatchStrategy<out Event>>
    ): EventRepository {
        val allStrategies = strategies + blockingStrategies.map { strategy ->
            @Suppress("UNCHECKED_CAST")
            (strategy as BlockingFetchBatchStrategy<Event>).asSuspendingFetchBatchStrategy()
        }
        return JdbcEventRepository(
            jdbc = jdbc,
            aggregates = aggregates,
            transactionTemplate = TransactionTemplate(transactionManager),
            properties = properties,
            strategiesByEventType = allStrategies.associateBy { it.eventType }
        )
    }
}
