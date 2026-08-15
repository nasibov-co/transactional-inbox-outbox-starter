package com.fnasibov.transactional.inbox.outbox.demo.jdbc

import com.fnasibov.transactional.inbox.outbox.core.api.model.BaseEvent
import org.springframework.data.relational.core.mapping.Table

@Table("jdbc_demo_events")
data class JdbcDemoEvent(
    val payload: String,
    val priority: Int = 0
) : BaseEvent()
