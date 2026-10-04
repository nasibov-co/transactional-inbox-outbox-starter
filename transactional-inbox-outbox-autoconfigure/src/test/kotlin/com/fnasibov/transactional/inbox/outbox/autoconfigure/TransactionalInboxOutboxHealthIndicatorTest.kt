package com.fnasibov.transactional.inbox.outbox.autoconfigure

import com.fnasibov.transactional.inbox.outbox.core.domain.EventProcessor
import com.fnasibov.transactional.inbox.outbox.core.domain.EventProcessorStarter
import io.mockk.mockk
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import org.junit.jupiter.api.Test
import org.springframework.boot.health.contributor.Status
import kotlin.test.assertEquals

class TransactionalInboxOutboxHealthIndicatorTest {

    @Test
    fun `health is up while the processor is running`() {
        val processor = mockk<EventProcessor>(relaxed = true)
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        try {
            val starter = EventProcessorStarter(processor, scope)
            starter.start()

            val health = requireNotNull(TransactionalInboxOutboxHealthIndicator(starter).health())

            assertEquals(Status.UP, health.status)
            assertEquals("running", health.details["processor"])
        } finally {
            scope.cancel()
        }
    }

    @Test
    fun `health is out of service once the processor is stopped`() {
        val processor = mockk<EventProcessor>(relaxed = true)
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        try {
            val starter = EventProcessorStarter(processor, scope)
            starter.start()
            starter.stop()

            val health = requireNotNull(TransactionalInboxOutboxHealthIndicator(starter).health())

            assertEquals(Status.OUT_OF_SERVICE, health.status)
            assertEquals("stopped", health.details["processor"])
        } finally {
            scope.cancel()
        }
    }
}
