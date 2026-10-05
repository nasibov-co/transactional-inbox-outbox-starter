package com.fnasibov.transactional.inbox.outbox.demo.jdbc

import com.fnasibov.transactional.inbox.outbox.core.api.model.BaseEvent
import com.fnasibov.transactional.inbox.outbox.core.api.model.BatchKey
import org.springframework.data.relational.core.mapping.Table

/**
 * Demo event processed by a [com.fnasibov.transactional.inbox.outbox.core.api.BatchEventHandler].
 *
 * The `@BatchKey` property groups events that must be handled together. All events
 * sharing the same [batchKey] value are fetched as one batch and reach
 * `BatchEventHandler.handleBatch` in a single invocation.
 */
@Table("batch_jdbc_demo_events")
data class BatchJdbcDemoEvent(
    @BatchKey
    val batchKey: String,
    val payload: String
) : BaseEvent()
