package com.fnasibov.transactional.inbox.outbox.starter.r2dbc.api

import com.fnasibov.transactional.inbox.outbox.starter.r2dbc.api.model.Event
import com.fnasibov.transactional.inbox.outbox.starter.r2dbc.configuration.TransactionalProperties
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.transaction.reactive.TransactionalOperator

interface FetchBatchStrategy<E : Event> {
    val eventType: Class<E>
    suspend fun fetchBatch(): List<E>
}