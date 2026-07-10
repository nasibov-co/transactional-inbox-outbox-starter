package com.fnasibov.transactional.inbox.outbox.starter.r2dbc.configuration

import com.fnasibov.transactional.inbox.outbox.starter.r2dbc.api.model.BaseEvent
import com.fnasibov.transactional.inbox.outbox.starter.r2dbc.domain.EventProcessor
import com.fnasibov.transactional.inbox.outbox.starter.r2dbc.domain.EventProcessorStarter
import com.fnasibov.transactional.inbox.outbox.starter.r2dbc.domain.EventRepository
import io.mockk.mockk
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.core.io.ClassPathResource
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.transaction.ReactiveTransaction
import org.springframework.transaction.ReactiveTransactionManager
import org.springframework.transaction.TransactionDefinition
import reactor.core.publisher.Mono
import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class TransactionalInboxOutboxAutoconfigurationTest {

    private val contextRunner = ApplicationContextRunner()
        .withUserConfiguration(R2dbcInfrastructureConfiguration::class.java)
        .withConfiguration(
            org.springframework.boot.autoconfigure.AutoConfigurations.of(
                TransactionalInboxOutboxAutoconfiguration::class.java,
                TransactionalInboxOutboxInfrastructureAutoConfiguration::class.java,
                TransactionalInboxOutboxRepositoryAutoConfiguration::class.java,
                TransactionalInboxOutboxProcessorAutoConfiguration::class.java,
                TransactionalInboxOutboxProcessorStarterAutoConfiguration::class.java,
                TransactionalInboxOutboxActuatorAutoConfiguration::class.java
            )
        )

    @Test
    fun `does not create processor when disabled`() {
        contextRunner.run { context ->
            assertFalse(context.containsBean("eventProcessor"))
            assertFalse(context.containsBean("eventProcessorStarter"))
        }
    }

    @Test
    fun `creates starter infrastructure when enabled`() {
        contextRunner
            .withPropertyValues("transactional.enabled=true")
            .run { context ->
                assertNotNull(context.getBean(TransactionalProperties::class.java))
                assertNotNull(context.getBean(EventRepository::class.java))
                assertNotNull(context.getBean(EventProcessor::class.java))
                assertNotNull(context.getBean(EventProcessorStarter::class.java))
            }
    }

    @Test
    fun `binds duration based properties`() {
        contextRunner
            .withPropertyValues(
                "transactional.enabled=true",
                "transactional.polling.active-interval=250ms",
                "transactional.polling.max-idle-interval=10s",
                "transactional.processing.shutdown-timeout=5s",
                "transactional.retry.initial-delay=2s",
                "transactional.retry.max-delay=30s",
                "transactional.retry.multiplier=3.0"
            )
            .run { context ->
                val properties = context.getBean(TransactionalProperties::class.java)

                assertEquals(250, properties.polling.activeInterval.toMillis())
                assertEquals(10, properties.polling.maxIdleInterval.seconds)
                assertEquals(5, properties.processing.shutdownTimeout.seconds)
                assertEquals(2, properties.retry.initialDelay.seconds)
                assertEquals(30, properties.retry.maxDelay.seconds)
                assertEquals(3.0, properties.retry.multiplier)
            }
    }

    @Test
    fun `binds event type processing properties and falls back to default concurrency`() {
        contextRunner
            .withPropertyValues(
                "transactional.enabled=true",
                "transactional.processing.concurrency=2",
                "transactional.processing.event-types[0].event-type=BindingEvent",
                "transactional.processing.event-types[0].concurrency=7",
                "transactional.processing.event-types[1].event-type=${QualifiedBindingEvent::class.java.name}",
                "transactional.processing.event-types[1].concurrency=3"
            )
            .run { context ->
                val properties = context.getBean(TransactionalProperties::class.java)

                assertEquals(7, properties.processing.concurrencyFor(BindingEvent::class.java))
                assertEquals(3, properties.processing.concurrencyFor(QualifiedBindingEvent::class.java))
                assertEquals(2, properties.processing.concurrencyFor(FallbackBindingEvent::class.java))
            }
    }

    @Test
    fun `retryFor returns global retry when event type has no override`() {
        val defaultRetry = TransactionalProperties.Retry(
            maxAttempts = 3,
            initialDelay = Duration.ofSeconds(1),
            multiplier = 2.0,
            maxDelay = Duration.ofMinutes(1)
        )

        val retry = TransactionalProperties.Processing()
            .retryFor(FallbackBindingEvent::class.java, defaultRetry)

        assertRetry(
            retry = retry,
            maxAttempts = 3,
            initialDelay = Duration.ofSeconds(1),
            multiplier = 2.0,
            maxDelay = Duration.ofMinutes(1)
        )
    }

    @Test
    fun `retryFor returns full event type retry override`() {
        val defaultRetry = TransactionalProperties.Retry(
            maxAttempts = 3,
            initialDelay = Duration.ofSeconds(1),
            multiplier = 2.0,
            maxDelay = Duration.ofMinutes(1)
        )
        val processing = TransactionalProperties.Processing(
            eventTypes = listOf(
                TransactionalProperties.EventTypeProcessing(
                    eventType = BindingEvent::class.java.simpleName,
                    retry = TransactionalProperties.EventTypeRetry(
                        maxAttempts = 10,
                        initialDelay = Duration.ofSeconds(5),
                        multiplier = 1.5,
                        maxDelay = Duration.ofMinutes(10)
                    )
                )
            )
        )

        val retry = processing.retryFor(BindingEvent::class.java, defaultRetry)

        assertRetry(
            retry = retry,
            maxAttempts = 10,
            initialDelay = Duration.ofSeconds(5),
            multiplier = 1.5,
            maxDelay = Duration.ofMinutes(10)
        )
    }

    @Test
    fun `retryFor merges partial event type retry with global retry`() {
        val defaultRetry = TransactionalProperties.Retry(
            maxAttempts = 3,
            initialDelay = Duration.ofSeconds(1),
            multiplier = 2.0,
            maxDelay = Duration.ofMinutes(1)
        )
        val processing = TransactionalProperties.Processing(
            eventTypes = listOf(
                TransactionalProperties.EventTypeProcessing(
                    eventType = BindingEvent::class.java.simpleName,
                    retry = TransactionalProperties.EventTypeRetry(
                        maxAttempts = 10
                    )
                )
            )
        )

        val retry = processing.retryFor(BindingEvent::class.java, defaultRetry)

        assertRetry(
            retry = retry,
            maxAttempts = 10,
            initialDelay = Duration.ofSeconds(1),
            multiplier = 2.0,
            maxDelay = Duration.ofMinutes(1)
        )
    }

    @Test
    fun `retryFor matches by fully qualified name`() {
        val defaultRetry = TransactionalProperties.Retry()
        val processing = TransactionalProperties.Processing(
            eventTypes = listOf(
                TransactionalProperties.EventTypeProcessing(
                    eventType = QualifiedBindingEvent::class.java.name,
                    retry = TransactionalProperties.EventTypeRetry(
                        maxAttempts = 5
                    )
                )
            )
        )

        val retry = processing.retryFor(QualifiedBindingEvent::class.java, defaultRetry)

        assertEquals(5, retry.maxAttempts)
    }

    @Test
    fun `retryFor matches by simple name`() {
        val defaultRetry = TransactionalProperties.Retry()
        val processing = TransactionalProperties.Processing(
            eventTypes = listOf(
                TransactionalProperties.EventTypeProcessing(
                    eventType = BindingEvent::class.java.simpleName,
                    retry = TransactionalProperties.EventTypeRetry(
                        maxAttempts = 5
                    )
                )
            )
        )

        val retry = processing.retryFor(BindingEvent::class.java, defaultRetry)

        assertEquals(5, retry.maxAttempts)
    }

    @Test
    fun `fails fast on invalid numeric properties`() {
        contextRunner
            .withPropertyValues(
                "transactional.enabled=true",
                "transactional.polling.batch-size=0"
            )
            .run { context ->
                assertTrue(context.startupFailure is Exception)
            }
    }

    @Test
    fun `backs off when user provides repository and coroutine scope`() {
        contextRunner
            .withUserConfiguration(CustomInfrastructureConfiguration::class.java)
            .withPropertyValues("transactional.enabled=true")
            .run { context ->
                assertEquals(
                    CustomInfrastructureConfiguration.customRepository,
                    context.getBean(EventRepository::class.java)
                )
                assertEquals(
                    CustomInfrastructureConfiguration.customScope,
                    context.getBean("transactionalCoroutineScope")
                )
            }
    }

    @Test
    fun `auto configuration imports define explicit loading order`() {
        val imports = ClassPathResource(
            "META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports"
        ).inputStream.bufferedReader().use { reader ->
            reader.readLines()
                .filter { line -> line.isNotBlank() }
        }

        assertEquals(
            listOf(
                TransactionalInboxOutboxAutoconfiguration::class.qualifiedName,
                TransactionalInboxOutboxInfrastructureAutoConfiguration::class.qualifiedName,
                TransactionalInboxOutboxRepositoryAutoConfiguration::class.qualifiedName,
                TransactionalInboxOutboxProcessorAutoConfiguration::class.qualifiedName,
                TransactionalInboxOutboxProcessorStarterAutoConfiguration::class.qualifiedName,
                TransactionalInboxOutboxActuatorAutoConfiguration::class.qualifiedName
            ),
            imports
        )
    }

    @Configuration(proxyBeanMethods = false)
    class R2dbcInfrastructureConfiguration {

        @Bean
        fun r2dbcEntityTemplate(): R2dbcEntityTemplate =
            mockk(relaxed = true)

        @Bean
        fun reactiveTransactionManager(): ReactiveTransactionManager =
            object : ReactiveTransactionManager {
                override fun getReactiveTransaction(
                    definition: TransactionDefinition?
                ): Mono<ReactiveTransaction> =
                    Mono.just(mockk(relaxed = true))

                override fun commit(transaction: ReactiveTransaction): Mono<Void> =
                    Mono.empty()

                override fun rollback(transaction: ReactiveTransaction): Mono<Void> =
                    Mono.empty()
            }
    }

    @Configuration(proxyBeanMethods = false)
    class CustomInfrastructureConfiguration {

        @Bean("transactionalCoroutineScope")
        fun transactionalCoroutineScope(): CoroutineScope =
            customScope

        @Bean
        fun eventRepository(): EventRepository =
            customRepository

        companion object {
            val customScope: CoroutineScope = CoroutineScope(Dispatchers.Unconfined)
            val customRepository: EventRepository = mockk(relaxed = true)
        }
    }

    private class BindingEvent : BaseEvent()

    private class QualifiedBindingEvent : BaseEvent()

    private class FallbackBindingEvent : BaseEvent()

    private fun assertRetry(
        retry: TransactionalProperties.ResolvedRetry,
        maxAttempts: Int,
        initialDelay: Duration,
        multiplier: Double,
        maxDelay: Duration
    ) {
        assertEquals(maxAttempts, retry.maxAttempts)
        assertEquals(initialDelay, retry.initialDelay)
        assertEquals(multiplier, retry.multiplier)
        assertEquals(maxDelay, retry.maxDelay)
    }
}
