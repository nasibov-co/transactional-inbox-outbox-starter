package com.fnasibov.transactional.inbox.outbox.core.api

import com.fnasibov.transactional.inbox.outbox.core.api.model.Event

/**
 * Blocking alternative to [FetchBatchStrategy] for custom JDBC-style polling.
 * Implementations are executed on [kotlinx.coroutines.Dispatchers.IO].
 */
interface BlockingFetchBatchStrategy<E : Event> {

    val eventType: Class<E>

    fun fetchBatch(): List<E>
}
