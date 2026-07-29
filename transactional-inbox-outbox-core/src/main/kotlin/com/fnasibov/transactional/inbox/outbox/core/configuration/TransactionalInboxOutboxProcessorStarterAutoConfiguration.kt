package com.fnasibov.transactional.inbox.outbox.core.configuration

import com.fnasibov.transactional.inbox.outbox.core.domain.EventProcessor
import com.fnasibov.transactional.inbox.outbox.core.domain.EventProcessorStarter
import kotlinx.coroutines.CoroutineScope
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean

@AutoConfiguration(after = [TransactionalInboxOutboxProcessorAutoConfiguration::class])
@ConditionalOnProperty(
    "transactional.enabled",
    havingValue = "true",
    matchIfMissing = false
)
@ConditionalOnBean(EventProcessor::class)
class TransactionalInboxOutboxProcessorStarterAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    fun eventProcessorStarter(
        processor: EventProcessor,
        @Qualifier("transactionalCoroutineScope")
        transactionalCoroutineScope: CoroutineScope
    ): EventProcessorStarter =
        EventProcessorStarter(processor, transactionalCoroutineScope)
}
