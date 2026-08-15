package com.fnasibov.transactional.inbox.outbox.autoconfigure

import com.fnasibov.transactional.inbox.outbox.core.domain.EventRepository
import com.fnasibov.transactional.inbox.outbox.jdbc.JdbcEventRepository
import com.fnasibov.transactional.inbox.outbox.r2dbc.R2dbcEventRepository
import io.mockk.mockk
import org.junit.jupiter.api.Test
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.jdbc.core.JdbcAggregateOperations
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.ReactiveTransactionManager
import kotlin.test.assertFalse
import kotlin.test.assertIs

class TransactionalRepositoryAutoConfigurationTest {

    @Test
    fun `creates r2dbc repository when reactive infrastructure is provided`() {
        ApplicationContextRunner()
            .withUserConfiguration(R2dbcInfrastructure::class.java)
            .withConfiguration(
                AutoConfigurations.of(
                    TransactionalInboxOutboxAutoconfiguration::class.java,
                    TransactionalInboxOutboxR2dbcAutoConfiguration::class.java
                )
            )
            .withPropertyValues("transactional.enabled=true")
            .run { context ->
                assertIs<R2dbcEventRepository>(context.getBean(EventRepository::class.java))
            }
    }

    @Test
    fun `creates jdbc repository when blocking infrastructure is provided`() {
        ApplicationContextRunner()
            .withUserConfiguration(JdbcInfrastructure::class.java)
            .withConfiguration(
                AutoConfigurations.of(
                    TransactionalInboxOutboxAutoconfiguration::class.java,
                    TransactionalInboxOutboxJdbcAutoConfiguration::class.java
                )
            )
            .withPropertyValues("transactional.enabled=true")
            .run { context ->
                assertIs<JdbcEventRepository>(context.getBean(EventRepository::class.java))
            }
    }

    @Test
    fun `does not create repository when starter is disabled`() {
        ApplicationContextRunner()
            .withUserConfiguration(JdbcInfrastructure::class.java)
            .withConfiguration(
                AutoConfigurations.of(
                    TransactionalInboxOutboxAutoconfiguration::class.java,
                    TransactionalInboxOutboxJdbcAutoConfiguration::class.java
                )
            )
            .run { context ->
                assertFalse(context.containsBean("jdbcEventRepository"))
            }
    }

    @Configuration(proxyBeanMethods = false)
    class R2dbcInfrastructure {
        @Bean
        fun r2dbcEntityTemplate(): R2dbcEntityTemplate = mockk(relaxed = true)

        @Bean
        fun reactiveTransactionManager(): ReactiveTransactionManager = mockk()
    }

    @Configuration(proxyBeanMethods = false)
    class JdbcInfrastructure {
        @Bean
        fun jdbc(): NamedParameterJdbcOperations = mockk()

        @Bean
        fun aggregates(): JdbcAggregateOperations = mockk()

        @Bean
        fun transactionManager(): PlatformTransactionManager = mockk()
    }
}
