package com.fnasibov.transactional.inbox.outbox.core.configuration

import com.fnasibov.transactional.inbox.outbox.core.domain.EventProcessingMetrics
import io.micrometer.core.instrument.MeterRegistry
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean

@AutoConfiguration(after = [TransactionalInboxOutboxAutoconfiguration::class])
@ConditionalOnProperty(
    "transactional.enabled",
    havingValue = "true",
    matchIfMissing = false
)
class TransactionalInboxOutboxInfrastructureAutoConfiguration {

    @Bean("transactionalCoroutineScope")
    @ConditionalOnMissingBean(name = ["transactionalCoroutineScope"])
    fun transactionalCoroutineScope(): CoroutineScope =
        CoroutineScope(SupervisorJob() + Dispatchers.Default)

    @Bean
    @ConditionalOnBean(MeterRegistry::class)
    @ConditionalOnMissingBean
    fun eventProcessingMetrics(
        meterRegistry: MeterRegistry
    ): EventProcessingMetrics =
        EventProcessingMetrics(meterRegistry)
}
