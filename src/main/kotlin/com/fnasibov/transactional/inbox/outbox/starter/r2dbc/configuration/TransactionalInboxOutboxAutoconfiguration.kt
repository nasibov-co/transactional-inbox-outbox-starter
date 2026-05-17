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

/**
 * Auto-configuration for the Transactional Inbox/Outbox starter.
 *
 * This configuration enables and wires all core infrastructure components:
 * - Event repository with optional per-event fetch strategies
 * - Event processor with handler mapping
 * - Background processing coroutine scope
 * - Processor lifecycle starter
 *
 * The configuration is activated only when:
 * `transactional.enabled = true`
 *
 * It is designed to be used as a Spring Boot starter module
 * and should not require manual bean wiring in client applications.
 */
@AutoConfiguration
@ConditionalOnProperty(
    "transactional.enabled",
    havingValue = "true",
    matchIfMissing = false
)
class TransactionalInboxOutboxAutoconfiguration(
    private val handlers: List<EventHandler<out Event>>
) {

    /**
     * Binds external configuration properties under `transactional.*`.
     *
     * @return strongly typed configuration object
     */
    @Bean
    @ConfigurationProperties(prefix = "transactional")
    fun transactionalProperty(): TransactionalProperties =
        TransactionalProperties()

    /**
     * Coroutine scope used for asynchronous event processing.
     *
     * Uses `SupervisorJob` to ensure failure isolation between coroutines
     * and `Dispatchers.IO` for blocking-friendly execution.
     *
     * @return shared coroutine scope for processing pipeline
     */
    @Bean
    fun transactionalCoroutineScope(): CoroutineScope =
        CoroutineScope(SupervisorJob() + Dispatchers.IO)

    /**
     * Creates the default event repository implementation.
     *
     * This repository supports:
     * - transactional batch polling
     * - optional custom fetch strategies per event type
     * - default fallback fetch logic
     *
     * @param template R2DBC database template
     * @param reactiveTransactionManager reactive transaction manager
     * @param properties transactional configuration properties
     * @param strategies optional fetch override strategies
     */
    @Bean
    fun eventRepository(
        template: R2dbcEntityTemplate,
        reactiveTransactionManager: ReactiveTransactionManager,
        properties: TransactionalProperties,
        strategies: List<FetchBatchStrategy<out Event>>
    ): BaseEventRepository {

        val strategiesByEventType =
            strategies.associateBy { it.eventType }

        val transactionalOperator =
            TransactionalOperator.create(reactiveTransactionManager)

        return BaseEventRepository(
            template = template,
            properties = properties,
            transactionalOperator = transactionalOperator,
            strategiesByEventType = strategiesByEventType
        )
    }

    /**
     * Creates the event processor responsible for executing handlers
     * for fetched events.
     *
     * Handlers are grouped by supported event type and executed
     * according to processing configuration.
     *
     * @param transactionalProperties processing configuration
     * @param repository event repository
     * @param transactionalCoroutineScope shared execution scope
     * @return configured event processor
     */
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

        return EventProcessor(
            handlerMap,
            repository,
            transactionalProperties,
            transactionalCoroutineScope
        )
    }

    /**
     * Starts the event processing lifecycle on application startup.
     *
     * Responsible for triggering polling loops and dispatching
     * events to the processor.
     *
     * @param processor event processor
     * @param transactionalCoroutineScope shared execution scope
     * @return processor starter bean
     */
    @Bean
    fun eventProcessorStarter(
        processor: EventProcessor,
        @Qualifier("transactionalCoroutineScope")
        transactionalCoroutineScope: CoroutineScope
    ): EventProcessorStarter =
        EventProcessorStarter(processor, transactionalCoroutineScope)
}