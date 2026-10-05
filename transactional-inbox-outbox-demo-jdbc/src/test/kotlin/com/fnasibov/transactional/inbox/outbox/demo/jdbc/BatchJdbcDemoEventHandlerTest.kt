package com.fnasibov.transactional.inbox.outbox.demo.jdbc

import com.fnasibov.transactional.inbox.outbox.core.api.model.BatchEventOutcome
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import java.util.UUID
import kotlin.test.assertEquals

class BatchJdbcDemoEventHandlerTest {

    private val handler = BatchJdbcDemoEventHandler()

    @Test
    fun `handleBatch reports every received event as processed`() = runBlocking {
        val first = BatchJdbcDemoEvent(batchKey = "account-42", payload = "first")
            .apply { id = UUID.randomUUID() }
        val second = BatchJdbcDemoEvent(batchKey = "account-42", payload = "second")
            .apply { id = UUID.randomUUID() }

        val result = handler.handleBatch(listOf(first, second))

        // The result must cover exactly the batch ids, otherwise the worker rejects it.
        assertEquals(setOf(first.id, second.id), result.outcomes.keys)
        assertEquals(BatchEventOutcome.PROCESSED, result.outcomeFor(first))
        assertEquals(BatchEventOutcome.PROCESSED, result.outcomeFor(second))
    }
}
