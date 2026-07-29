package com.fnasibov.transactional.inbox.outbox.core.configuration

import com.fnasibov.transactional.inbox.outbox.core.domain.EventProcessorStarter
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean

@AutoConfiguration(after = [TransactionalInboxOutboxProcessorStarterAutoConfiguration::class])
@ConditionalOnProperty(
    "transactional.enabled",
    havingValue = "true",
    matchIfMissing = false
)
@ConditionalOnClass(
    name = ["org.springframework.boot.health.contributor.HealthIndicator"]
)
class TransactionalInboxOutboxActuatorAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(name = ["transactionalInboxOutboxHealthIndicator"])
    fun transactionalInboxOutboxHealthIndicator(
        starter: EventProcessorStarter
    ): TransactionalInboxOutboxHealthIndicator =
        TransactionalInboxOutboxHealthIndicator(starter)
}
