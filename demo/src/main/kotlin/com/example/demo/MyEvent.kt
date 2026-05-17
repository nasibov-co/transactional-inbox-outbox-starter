package com.example.demo

import com.fnasibov.transactional.inbox.outbox.starter.r2dbc.api.model.Event
import com.fnasibov.transactional.inbox.outbox.starter.r2dbc.api.model.EventStatus
import org.springframework.data.relational.core.mapping.Table
import java.time.ZonedDateTime
import java.util.UUID

@Table("my_events")
data class MyEvent(
    override val id: UUID = UUID.randomUUID(),
    override val aggregateType: String = "MY_AGGREGATE",
    override val aggregateId: String,
    override val payload: String,
    override val schemaVersion: String? = "1.0",
    override val status: EventStatus = com.fnasibov.transactional.inbox.outbox.starter.r2dbc.api.model.EventStatus.PENDING,
    override val createdAt: ZonedDateTime = ZonedDateTime.now(),
    override val updatedAt: ZonedDateTime? = null,
    override val retryCount: Int = 0,
    override val lastAttemptAt: ZonedDateTime? = null
) : Event {
}