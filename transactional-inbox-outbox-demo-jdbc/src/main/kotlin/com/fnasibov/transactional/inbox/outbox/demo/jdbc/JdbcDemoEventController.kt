package com.fnasibov.transactional.inbox.outbox.demo.jdbc

import com.fnasibov.transactional.inbox.outbox.core.api.model.EventStatus
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import java.util.UUID

@RestController
@RequestMapping("/demo-events")
class JdbcDemoEventController(
    private val repository: JdbcDemoEventRepository
) {

    @PostMapping
    fun createEvent(
        @RequestBody request: CreateJdbcDemoEventRequest
    ): JdbcDemoEventResponse {
        val saved = repository.save(
            JdbcDemoEvent(
                payload = request.payload,
                priority = request.priority
            )
        )

        return JdbcDemoEventResponse(
            id = saved.id,
            status = saved.status,
            payload = saved.payload,
            priority = saved.priority
        )
    }
}

data class CreateJdbcDemoEventRequest(
    val payload: String,
    val priority: Int = 0
)

data class JdbcDemoEventResponse(
    val id: UUID?,
    val status: EventStatus,
    val payload: String,
    val priority: Int
)
