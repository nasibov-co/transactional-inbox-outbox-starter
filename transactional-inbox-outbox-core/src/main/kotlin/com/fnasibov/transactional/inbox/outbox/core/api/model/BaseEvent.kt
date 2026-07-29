package com.fnasibov.transactional.inbox.outbox.core.api.model

import org.springframework.data.annotation.Id
import java.time.ZonedDateTime
import java.util.UUID

/**
 * Base implementation of [Event] with default lifecycle fields.
 *
 * Extend this class when an event only needs to add domain-specific columns:
 *
 * ```
 * @Table("payment_events")
 * data class PaymentEvent(
 *     val paymentId: UUID,
 *     val amount: Long
 * ) : BaseEvent()
 * ```
 */
abstract class BaseEvent(
    @Id
    override var id: UUID? = null,
    override var status: EventStatus = EventStatus.PENDING,
    override var createdAt: ZonedDateTime = ZonedDateTime.now(),
    override var updatedAt: ZonedDateTime? = null,
    override var retryCount: Int = 0,
    override var lastAttemptAt: ZonedDateTime? = null,
    override var nextRetryAt: ZonedDateTime? = null
) : Event
