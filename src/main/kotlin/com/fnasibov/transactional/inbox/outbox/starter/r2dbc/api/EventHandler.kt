package com.fnasibov.transactional.inbox.outbox.starter.r2dbc.api

import com.fnasibov.transactional.inbox.outbox.starter.r2dbc.api.model.Event

/**
 * Interface for handling outbox events.
 *
 * Implementations of this interface are responsible for processing specific types of events
 * retrieved from the outbox table. Typically, this involves publishing the event payload
 * to a message broker (e.g., Kafka, RabbitMQ).
 *
 * @param E The type of the event this handler processes. Must be a subclass of [Event].
 */
interface EventHandler<E : Event> {

    /**
     * Returns the class type of the event this handler supports.
     * This is used by the dispatcher to route events to the correct handler.
     *
     * @return The class object of the supported event type.
     */
    fun supportedEventType(): Class<E>

    /**
     * Handles the given event.
     *
     * This method should contain the logic for sending the event to the external system.
     * It must be non-blocking that completes when the operation is done.
     *
     * @param event The event to handle.
     */
    suspend fun handle(event: E)

    /**
     * Handles failures that occur during event processing.
     *
     * This method is invoked when [handle] fails or when an unexpected exception
     * occurs during processing in the worker pipeline.
     *
     * It can be used for:
     * - logging and monitoring
     * - custom retry logic
     * - sending events to a dead-letter queue
     * - compensating actions
     *
     * @param event event that failed processing
     * @param error exception that caused the failure
     */
    suspend fun handleDeadLetter(event: E, error: Throwable)
}