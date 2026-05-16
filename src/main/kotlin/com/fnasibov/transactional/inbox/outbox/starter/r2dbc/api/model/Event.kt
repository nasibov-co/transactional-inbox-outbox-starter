package com.fnasibov.transactional.inbox.outbox.starter.r2dbc.api.model

import java.time.ZonedDateTime
import java.util.*

/**
 * Base model for Transactional Outbox events.
 *
 * This class is designed to be extended by domain-specific event models.
 * It contains common fields required for the outbox pattern:
 * - Identification (id, aggregateId, aggregateType)
 * - Payload data
 * - Lifecycle status (status, retryCount)
 * - Timestamps (createdAt, updatedAt)
 **/
interface Event{
    val id: UUID
    val aggregateType: String
    val aggregateId: String
    val payload: String
    val schemaVersion: String?
    val status: EventStatus
    val createdAt: ZonedDateTime
    val updatedAt: ZonedDateTime?
    val retryCount: Int
    val lastAttemptAt: ZonedDateTime?
}