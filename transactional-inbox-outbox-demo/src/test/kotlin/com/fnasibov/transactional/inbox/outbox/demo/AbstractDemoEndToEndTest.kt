package com.fnasibov.transactional.inbox.outbox.demo

import com.fnasibov.transactional.inbox.outbox.core.api.model.Event
import com.fnasibov.transactional.inbox.outbox.core.api.model.EventStatus
import kotlinx.coroutines.delay
import org.junit.jupiter.api.BeforeEach
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ContextConfiguration
import kotlin.test.fail
import java.util.UUID

/**
 * Base class for the demo end-to-end suites.
 *
 * Boots the real [DemoApplication] (including the starter's polling and processing
 * pipeline) against a shared Testcontainers PostgreSQL database. Events are written
 * through `repository.save`; controllers are intentionally not exercised.
 */
@SpringBootTest(classes = [DemoApplication::class, DemoEndToEndTestConfig::class])
@ContextConfiguration(initializers = [DemoPostgresInitializer::class])
abstract class AbstractDemoEndToEndTest {

    @BeforeEach
    fun resetRecordedHandlerInvocations() {
        HandlerInvocationRecorder.clear()
    }

    /**
     * Polls the event row until it reaches [expected] and returns the up-to-date entity.
     */
    protected suspend fun awaitStatus(
        id: UUID,
        expected: EventStatus,
        find: suspend (UUID) -> Event?,
    ): Event = awaitValue("event $id to reach status $expected") {
        find(id)?.takeIf { it.status == expected }
    }

    /**
     * Polls [condition] until it returns a non-null value and returns that value.
     */
    protected suspend fun <T> awaitValue(
        description: String,
        timeoutMillis: Long = 30_000,
        pollIntervalMillis: Long = 100,
        condition: suspend () -> T?,
    ): T {
        val deadline = System.currentTimeMillis() + timeoutMillis
        while (true) {
            condition()?.let { return it }
            if (System.currentTimeMillis() >= deadline) {
                fail("Timed out waiting for $description")
            }
            delay(pollIntervalMillis)
        }
    }

    /**
     * Polls [condition] until it holds.
     */
    protected suspend fun awaitThat(
        description: String,
        timeoutMillis: Long = 30_000,
        pollIntervalMillis: Long = 100,
        condition: suspend () -> Boolean,
    ): Unit = awaitValue(description, timeoutMillis, pollIntervalMillis) {
        if (condition()) Unit else null
    }
}
