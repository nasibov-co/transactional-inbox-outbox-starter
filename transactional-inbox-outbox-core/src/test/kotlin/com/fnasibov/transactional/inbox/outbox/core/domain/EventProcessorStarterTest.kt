package com.fnasibov.transactional.inbox.outbox.core.domain

import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.mockk
import io.mockk.verify
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.isActive
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class EventProcessorStarterTest {

    @Test
    fun `start is idempotent and reports the processor running`() {
        val processor = mockk<EventProcessor>(relaxed = true)
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        try {
            val starter = EventProcessorStarter(processor, scope)

            assertFalse(starter.isRunning())
            starter.start()
            starter.start()

            assertTrue(starter.isRunning())
            verify(exactly = 1) { processor.start() }
        } finally {
            scope.cancel()
        }
    }

    @Test
    fun `stop stops the processor, cancels the scope and reports not running`() {
        val processor = mockk<EventProcessor>(relaxed = true)
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val starter = EventProcessorStarter(processor, scope)

        starter.start()
        starter.stop()

        assertFalse(starter.isRunning())
        assertFalse(scope.isActive)
        verify(exactly = 1) { processor.stop() }
    }

    @Test
    fun `graceful stop stops the processor and runs the callback`() {
        val processor = mockk<EventProcessor>(relaxed = true)
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        try {
            val starter = EventProcessorStarter(processor, scope)
            val stopped = CompletableDeferred<Unit>()

            starter.start()
            starter.stop(Runnable { stopped.complete(Unit) })

            runBlocking {
                withTimeout(1_000) { stopped.await() }
            }

            assertFalse(starter.isRunning())
            assertFalse(scope.isActive)
            coVerify(exactly = 1) { processor.stopGracefully() }
        } finally {
            scope.cancel()
        }
    }

    @Test
    fun `graceful stop failure still runs the callback`() {
        val processor = mockk<EventProcessor>(relaxed = true)
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        try {
            val starter = EventProcessorStarter(processor, scope)
            val stopped = CompletableDeferred<Unit>()
            val failure = IllegalStateException("graceful stop failed")
            coEvery { processor.stopGracefully() } throws failure

            starter.start()
            starter.stop(Runnable { stopped.complete(Unit) })

            runBlocking {
                withTimeout(1_000) { stopped.await() }
            }

            assertTrue(stopped.isCompleted)
            coVerify(exactly = 1) { processor.stopGracefully() }
        } finally {
            scope.cancel()
        }
    }

    @Test
    fun `starter auto starts with the processor stopped initially`() {
        val processor = mockk<EventProcessor>(relaxed = true)
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        try {
            val starter = EventProcessorStarter(processor, scope)

            assertTrue(starter.isAutoStartup())
            assertFalse(starter.isRunning())
        } finally {
            scope.cancel()
        }
    }
}
