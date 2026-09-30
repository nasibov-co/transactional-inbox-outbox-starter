package com.fnasibov.transactional.inbox.outbox.core.api.model

import org.junit.jupiter.api.Test
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull

class BatchResultTest {

    @Test
    fun `keeps outcomes keyed by event id`() {
        val id = UUID.randomUUID()

        val result = BatchResult.of(mapOf(id to BatchEventOutcome.RETRY))

        assertEquals(mapOf(id to BatchEventOutcome.RETRY), result.outcomes)
    }

    @Test
    fun `allProcessed marks every event processed`() {
        val first = TestEvent()
        val second = TestEvent()

        val result = BatchResult.allProcessed(listOf(first, second))

        assertEquals(BatchEventOutcome.PROCESSED, result.outcomeFor(first))
        assertEquals(BatchEventOutcome.PROCESSED, result.outcomeFor(second))
    }

    @Test
    fun `allRetried marks every event retryable`() {
        val first = TestEvent()
        val second = TestEvent()

        val result = BatchResult.allRetried(listOf(first, second))

        assertEquals(BatchEventOutcome.RETRY, result.outcomeFor(first))
        assertEquals(BatchEventOutcome.RETRY, result.outcomeFor(second))
    }

    @Test
    fun `outcome builders reject events without id`() {
        assertFailsWith<IllegalArgumentException> {
            BatchResult.allProcessed(listOf(TestEvent(id = null)))
        }
        assertFailsWith<IllegalArgumentException> {
            BatchResult.allRetried(listOf(TestEvent(id = null)))
        }
    }

    @Test
    fun `outcomeFor returns null for events without id`() {
        val result = BatchResult.of(mapOf(UUID.randomUUID() to BatchEventOutcome.PROCESSED))

        assertNull(result.outcomeFor(TestEvent(id = null)))
    }

    @Test
    fun `mergedWith keeps retry dominant so batches are never acknowledged early`() {
        val first = TestEvent()
        val second = TestEvent()
        val left = BatchResult.of(mapOf(first.id!! to BatchEventOutcome.PROCESSED))
        val right = BatchResult.of(
            mapOf(
                first.id!! to BatchEventOutcome.RETRY,
                second.id!! to BatchEventOutcome.PROCESSED
            )
        )

        val merged = left.mergedWith(right)

        assertEquals(BatchEventOutcome.RETRY, merged.outcomeFor(first))
        assertEquals(BatchEventOutcome.PROCESSED, merged.outcomeFor(second))
    }

    @Test
    fun `empty result carries no outcomes`() {
        assertEquals(emptyMap(), BatchResult.of(emptyMap()).outcomes)
    }

    private class TestEvent(id: UUID? = UUID.randomUUID()) : BaseEvent(id = id)
}
