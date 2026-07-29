package com.fnasibov.transactional.inbox.outbox.demo

import com.fnasibov.transactional.inbox.outbox.core.api.model.BaseEvent
import org.springframework.data.relational.core.mapping.Table

@Table("demo_events")
data class DemoEvent(
    val payload: String,
    val priority: Int = 0
) : BaseEvent()
