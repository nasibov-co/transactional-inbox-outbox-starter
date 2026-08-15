package com.fnasibov.transactional.inbox.outbox.core.api

import com.fnasibov.transactional.inbox.outbox.core.api.model.Event
import com.fnasibov.transactional.inbox.outbox.core.api.model.EventStatus
import com.fnasibov.transactional.inbox.outbox.core.domain.EventRepository
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runInterruptible

/** Adapts a blocking handler to the coroutine-based processing pipeline. */
fun <E : Event> BlockingEventHandler<E>.asSuspendingEventHandler(): EventHandler<E> =
    object : EventHandler<E> {
        override fun supportedEventType(): Class<E> =
            this@asSuspendingEventHandler.supportedEventType()

        override suspend fun handle(event: E) {
            runInterruptible(Dispatchers.IO) {
                this@asSuspendingEventHandler.handle(event)
            }
        }

        override suspend fun handleDeadLetter(event: E, error: Throwable) {
            runInterruptible(Dispatchers.IO) {
                this@asSuspendingEventHandler.handleDeadLetter(event, error)
            }
        }
    }

/** Adapts a blocking batch strategy to a suspending repository contract. */
fun <E : Event> BlockingFetchBatchStrategy<E>.asSuspendingFetchBatchStrategy(): FetchBatchStrategy<E> =
    object : FetchBatchStrategy<E> {
        override val eventType: Class<E> =
            this@asSuspendingFetchBatchStrategy.eventType

        override suspend fun fetchBatch(): List<E> =
            runInterruptible(Dispatchers.IO) {
                this@asSuspendingFetchBatchStrategy.fetchBatch()
            }
    }

/** Adapts a client-provided blocking repository to the processing pipeline. */
fun BlockingEventRepository.asSuspendingEventRepository(): EventRepository =
    object : EventRepository {
        override suspend fun <E : Event> fetchBatch(eventType: Class<E>): List<E> =
            runInterruptible(Dispatchers.IO) {
                this@asSuspendingEventRepository.fetchBatch(eventType)
            }

        override suspend fun <E : Event> markAsProcessed(event: E) {
            runInterruptible(Dispatchers.IO) {
                this@asSuspendingEventRepository.markAsProcessed(event)
            }
        }

        override suspend fun <E : Event> markAsDeadLetter(event: E) {
            runInterruptible(Dispatchers.IO) {
                this@asSuspendingEventRepository.markAsDeadLetter(event)
            }
        }

        override suspend fun <E : Event> markAsFailed(event: E): EventStatus =
            runInterruptible(Dispatchers.IO) {
                this@asSuspendingEventRepository.markAsFailed(event)
            }
    }
