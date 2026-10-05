package com.fnasibov.transactional.inbox.outbox.demo

import com.fnasibov.transactional.inbox.outbox.core.api.model.EventStatus
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.test.context.TestPropertySource
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

/**
 * End-to-end test for the dead-letter path of the R2DBC demo.
 *
 * The retry limit is lowered to two attempts so the failure-prone [DemoEventHandler]
 * (it throws while `retryCount < 2`) exhausts its retries: the first attempt fails and
 * is scheduled for retry, the second attempt fails as well, and the event is moved to
 * [EventStatus.DEAD_LETTER] where the handler's dead-letter callback receives it.
 */
@TestPropertySource(properties = ["transactional.retry.max-attempts=2"])
class DemoEventDeadLetterEndToEndTest : AbstractDemoEndToEndTest() {

    @Autowired
    private lateinit var demoEventRepository: DemoEventRepository

    @Test
    fun `saved demo event that exhausts retries is moved to dead letter`() = runBlocking {
        val saved = demoEventRepository.save(DemoEvent(payload = "e2e-dead-letter"))
        val id = assertNotNull(saved.id, "the saved event must receive an identifier")

        val deadLettered = awaitStatus(id, EventStatus.DEAD_LETTER, demoEventRepository::findById)

        assertEquals(
            2,
            deadLettered.retryCount,
            "the event must record one retry before the retry limit is exhausted"
        )
        assertEquals(
            listOf(0, 1),
            HandlerInvocationRecorder.regularAttemptsFor(id),
            "the handler must be invoked twice, each attempt observing a higher retry count"
        )
        awaitThat("the dead-letter callback to be invoked for event $id") {
            HandlerInvocationRecorder.regularDeadLetterCallsFor(id) == 1
        }
    }
}
