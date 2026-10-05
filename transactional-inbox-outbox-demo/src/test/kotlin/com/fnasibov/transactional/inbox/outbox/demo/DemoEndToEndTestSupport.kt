package com.fnasibov.transactional.inbox.outbox.demo

import com.fnasibov.transactional.inbox.outbox.core.api.BatchEventHandler
import com.fnasibov.transactional.inbox.outbox.core.api.EventHandler
import com.fnasibov.transactional.inbox.outbox.core.api.model.BatchResult
import com.fnasibov.transactional.inbox.outbox.core.api.model.Event
import org.reactivestreams.Publisher
import org.springframework.beans.factory.config.BeanPostProcessor
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.ApplicationContextInitializer
import org.springframework.context.ConfigurableApplicationContext
import org.springframework.context.annotation.Bean
import org.springframework.core.env.MapPropertySource
import org.springframework.data.r2dbc.mapping.event.BeforeConvertCallback
import org.springframework.data.relational.core.sql.SqlIdentifier
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.utility.DockerImageName
import reactor.core.publisher.Mono
import java.util.UUID

/**
 * Shared PostgreSQL instance for the demo end-to-end suites. Started lazily on first
 * use and reused by every test class in the module.
 */
class DemoPostgresContainer : PostgreSQLContainer<DemoPostgresContainer>(
    DockerImageName.parse("postgres:16-alpine")
)

object DemoPostgres {
    val instance: DemoPostgresContainer = DemoPostgresContainer()

    fun start() {
        if (!instance.isRunning) {
            instance.start()
        }
    }
}

/**
 * Points the application at the shared Testcontainers PostgreSQL instance.
 */
class DemoPostgresInitializer : ApplicationContextInitializer<ConfigurableApplicationContext> {

    override fun initialize(applicationContext: ConfigurableApplicationContext) {
        DemoPostgres.start()
        val postgres = DemoPostgres.instance
        applicationContext.environment.propertySources.addFirst(
            MapPropertySource(
                "demoPostgresContainer",
                mapOf(
                    "spring.r2dbc.url" to
                        "r2dbc:postgresql://${postgres.host}:${postgres.getMappedPort(5432)}/${postgres.databaseName}",
                    "spring.r2dbc.username" to postgres.username,
                    "spring.r2dbc.password" to postgres.password,
                ),
            )
        )
    }
}

/**
 * Thread-safe record of real handler invocations performed by the processing pipeline.
 *
 * The application's handler beans are wrapped with recording delegates by
 * [RecordingHandlerPostProcessor], so entries in this recorder prove that the production
 * dispatch, retry, and dead-letter paths actually invoked the demo handlers.
 */
object HandlerInvocationRecorder {

    private val lock = Any()
    private val regularAttempts = mutableMapOf<UUID?, MutableList<Int>>()
    private val regularDeadLetterCalls = mutableListOf<UUID?>()
    private val batchInvocations = mutableListOf<List<UUID?>>()
    private val batchDeadLetterCalls = mutableListOf<List<UUID?>>()

    fun clear() = synchronized(lock) {
        regularAttempts.clear()
        regularDeadLetterCalls.clear()
        batchInvocations.clear()
        batchDeadLetterCalls.clear()
    }

    fun recordRegularAttempt(id: UUID?, retryCount: Int): Unit = synchronized(lock) {
        regularAttempts.computeIfAbsent(id) { mutableListOf() }.add(retryCount)
    }

    fun recordRegularDeadLetter(id: UUID?): Unit = synchronized(lock) {
        regularDeadLetterCalls += id
    }

    fun recordBatchInvocation(eventIds: List<UUID?>): Unit = synchronized(lock) {
        batchInvocations += eventIds
    }

    fun recordBatchDeadLetter(eventIds: List<UUID?>): Unit = synchronized(lock) {
        batchDeadLetterCalls += eventIds
    }

    /** Retry counts observed at each [EventHandler.handle] invocation for [id]. */
    fun regularAttemptsFor(id: UUID): List<Int> = synchronized(lock) {
        regularAttempts[id]?.toList() ?: emptyList()
    }

    /** Number of [EventHandler.handleDeadLetter] invocations for [id]. */
    fun regularDeadLetterCallsFor(id: UUID): Int = synchronized(lock) {
        regularDeadLetterCalls.count { it == id }
    }

    /** Event ids of every recorded [BatchEventHandler.handleBatch] invocation. */
    fun batchInvocations(): List<List<UUID?>> = synchronized(lock) {
        batchInvocations.map { it.toList() }
    }
}

/**
 * Delegating [EventHandler] that records invocations before forwarding them to the
 * application's real handler.
 */
class RecordingEventHandler(
    private val delegate: EventHandler<Event>,
) : EventHandler<Event> {

    override fun supportedEventType(): Class<Event> = delegate.supportedEventType()

    override suspend fun handle(event: Event) {
        HandlerInvocationRecorder.recordRegularAttempt(event.id, event.retryCount)
        delegate.handle(event)
    }

    override suspend fun handleDeadLetter(event: Event, error: Throwable) {
        HandlerInvocationRecorder.recordRegularDeadLetter(event.id)
        delegate.handleDeadLetter(event, error)
    }
}

/**
 * Delegating [BatchEventHandler] that records invocations before forwarding them to the
 * application's real handler.
 */
class RecordingBatchEventHandler(
    private val delegate: BatchEventHandler<Event>,
) : BatchEventHandler<Event> {

    override fun supportedEventType(): Class<Event> = delegate.supportedEventType()

    override suspend fun handleBatch(events: List<Event>): BatchResult {
        HandlerInvocationRecorder.recordBatchInvocation(events.map { it.id })
        return delegate.handleBatch(events)
    }

    override suspend fun handleDeadLetter(events: List<Event>, error: Throwable) {
        HandlerInvocationRecorder.recordBatchDeadLetter(events.map { it.id })
        delegate.handleDeadLetter(events, error)
    }
}

/**
 * Replaces handler beans with recording delegates so tests can assert on real handler
 * invocations performed by the processing pipeline.
 */
class RecordingHandlerPostProcessor : BeanPostProcessor {

    @Suppress("UNCHECKED_CAST")
    override fun postProcessAfterInitialization(bean: Any, beanName: String): Any = when (bean) {
        is EventHandler<*> -> RecordingEventHandler(bean as EventHandler<Event>)
        is BatchEventHandler<*> -> RecordingBatchEventHandler(bean as BatchEventHandler<Event>)
        else -> bean
    }
}

/**
 * Test-only wiring for the demo end-to-end suites.
 */
@TestConfiguration(proxyBeanMethods = false)
class DemoEndToEndTestConfig {

    @Bean
    fun recordingHandlerPostProcessor(): BeanPostProcessor = RecordingHandlerPostProcessor()

    /**
     * Spring Data R2DBC persists a new entity with a null id by omitting the id column
     * and relying on database-generated values, which the demo tables do not provide.
     * These callbacks assign the identifier explicitly so `repository.save` works exactly
     * as the application code calls it. They are test-scoped and change no production
     * behavior.
     */
    @Bean
    fun demoEventIdCallback(): BeforeConvertCallback<DemoEvent> =
        object : BeforeConvertCallback<DemoEvent> {
            override fun onBeforeConvert(entity: DemoEvent, table: SqlIdentifier): Publisher<DemoEvent> {
                if (entity.id == null) {
                    entity.id = UUID.randomUUID()
                }
                return Mono.just(entity)
            }
        }

    @Bean
    fun batchDemoEventIdCallback(): BeforeConvertCallback<BatchDemoEvent> =
        object : BeforeConvertCallback<BatchDemoEvent> {
            override fun onBeforeConvert(entity: BatchDemoEvent, table: SqlIdentifier): Publisher<BatchDemoEvent> {
                if (entity.id == null) {
                    entity.id = UUID.randomUUID()
                }
                return Mono.just(entity)
            }
        }
}
