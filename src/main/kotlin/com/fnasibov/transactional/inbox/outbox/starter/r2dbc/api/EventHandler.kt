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
     * Handles errors that occur during event processing.
     *
     * This method is invoked if the [handle] method fails or if an unexpected exception occurs.
     * Implementations can define custom retry logic, logging, or dead-letter queue handling here.
     *
     * @param event The event that failed to process.
     * @param error The exception that occurred.
     */
    suspend fun handleError(event: E, error: Throwable)
}