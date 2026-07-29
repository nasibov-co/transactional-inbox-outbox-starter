package com.fnasibov.transactional.inbox.outbox.demo

import com.fnasibov.transactional.inbox.outbox.core.api.model.EventStatus
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import java.util.UUID

@RestController
@RequestMapping("/demo-events")
class DemoEventController(
    private val repository: DemoEventRepository
) {

    @PostMapping
    suspend fun createEvent(
        @RequestBody request: CreateDemoEventRequest
    ): DemoEventResponse {
        val event = DemoEvent(
            payload = request.payload,
            priority = request.priority
        )

        val saved = repository.save(event)

        return DemoEventResponse(
            id = saved.id,
            status = saved.status,
            payload = saved.payload,
            priority = saved.priority
        )
    }
}

data class CreateDemoEventRequest(
    val payload: String,
    val priority: Int = 0
)

data class DemoEventResponse(
    val id: UUID?,
    val status: EventStatus,
    val payload: String,
    val priority: Int
)
