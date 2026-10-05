package com.fnasibov.transactional.inbox.outbox.demo.jdbc

import com.fnasibov.transactional.inbox.outbox.core.api.model.EventStatus
import com.fnasibov.transactional.inbox.outbox.core.domain.EventProcessor
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

/**
 * End-to-end tests for the regular and batch processing paths of the JDBC demo.
 *
 * Events are persisted through `repository.save` and then travel the real polling,
 * dispatch, and status-transition pipeline. Controllers are not exercised. The
 * application's [JdbcDemoEventHandler] fails while `retryCount < 2`, so a saved
 * [JdbcDemoEvent] observably goes through two retries before the third attempt succeeds.
 */
class JdbcDemoEventProcessingEndToEndTest : AbstractJdbcDemoEndToEndTest() {

    @Autowired
    private lateinit var jdbcDemoEventRepository: JdbcDemoEventRepository

    @Autowired
    private lateinit var batchJdbcDemoEventRepository: BatchJdbcDemoEventRepository

    @Autowired
    private lateinit var processor: EventProcessor

    @Test
    fun `saved jdbc demo event is retried and then processed by the regular handler`() = runBlocking {
        val saved = jdbcDemoEventRepository.save(JdbcDemoEvent(payload = "e2e-regular"))
        val id = assertNotNull(saved.id, "the saved event must receive an identifier")

        val completed = awaitStatus(id, EventStatus.PROCESSED) {
            jdbcDemoEventRepository.findById(it).orElse(null)
        }

        assertEquals(
            2,
            completed.retryCount,
            "the handler must fail twice before the third attempt succeeds, leaving two recorded retries"
        )
        assertEquals(
            listOf(0, 1, 2),
            HandlerInvocationRecorder.regularAttemptsFor(id),
            "the handler must be invoked exactly three times, each attempt observing a higher retry count"
        )
        assertEquals(
            0,
            HandlerInvocationRecorder.regularDeadLetterCallsFor(id),
            "an eventually processed event must never reach the dead-letter callback"
        )
    }

    @Test
    fun `saved batch events reach the batch handler in one grouped invocation`() = runBlocking {
        val batchKey = "e2e-batch"
        val groupIds = mutableSetOf<UUID>()

        // Freeze polling while the fixture is arranged so the whole @BatchKey group
        // becomes visible to the fetch layer at once and is delivered in one invocation.
        processor.stop()
        try {
            repeat(3) { index ->
                val saved = batchJdbcDemoEventRepository.save(
                    BatchJdbcDemoEvent(batchKey = batchKey, payload = "e2e-batch-$index")
                )
                groupIds += assertNotNull(saved.id, "the saved batch event must receive an identifier")
            }
        } finally {
            processor.start()
        }

        groupIds.forEach { id ->
            val completed = awaitStatus(id, EventStatus.PROCESSED) {
                batchJdbcDemoEventRepository.findById(it).orElse(null)
            }
            assertEquals(0, completed.retryCount, "the batch handler must process every event without failures")
        }

        val deliveries = HandlerInvocationRecorder.batchInvocations()
            .map { invocation -> invocation.filterNotNull() }
            .filter { invocation -> invocation.any { it in groupIds } }

        assertEquals(
            1,
            deliveries.size,
            "the batchKey group must be delivered to handleBatch in a single invocation"
        )
        assertEquals(
            groupIds,
            deliveries.single().toSet(),
            "the single invocation must contain exactly the saved group"
        )
    }
}
