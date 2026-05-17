package com.fnasibov.transactional.inbox.outbox.starter.r2dbc.configuration

import java.time.Duration

/**
 * Configuration properties for transactional inbox/outbox processing.
 *
 * Defines polling, processing, and retry behavior
 * used by the event processing infrastructure.
 */
data class TransactionalProperties(

    /**
     * Enables or disables transactional inbox/outbox processing.
     */
    var enabled: Boolean = false,

    /**
     * Polling configuration.
     */
    var polling: Polling = Polling(),

    /**
     * Event processing configuration.
     */
    var processing: Processing = Processing(),

    /**
     * Retry configuration.
     */
    var retry: Retry = Retry()

) {

    /**
     * Polling behavior configuration.
     */
    data class Polling(

        /**
         * Maximum interval between polling cycles when no events are available.
         *
         * Default: 30 seconds.
         */
        var maxIdleIntervalMs: Duration = Duration.ofMillis(30000),

        /**
         * Polling interval while events are actively being processed.
         *
         * Default: 100 milliseconds.
         */
        var activeIntervalMs: Duration = Duration.ofMillis(100),

        /**
         * Maximum number of events fetched in a single batch.
         *
         * Default: 15.
         */
        var batchSize: Int = 15,

        /**
         * Maximum number of concurrently processed events within a batch.
         *
         * Default: 5.
         */
        var maxConcurrency: Int = 5,

        /**
         * Internal channel capacity used for buffering fetched events.
         *
         * Default: 25.
         */
        var channelCapacity: Int = 25
    )

    /**
     * Event processing execution settings.
     */
    data class Processing(

        /**
         * Number of concurrent event processing workers.
         *
         * Default: 5.
         */
        var concurrency: Int = 5
    )

    /**
     * Retry behavior configuration.
     */
    data class Retry(

        /**
         * Maximum number of retry attempts before moving
         * an event to dead-letter state.
         *
         * Default: 3.
         */
        var maxImmediateAttempts: Int = 3,

        /**
         * Initial retry delay used for exponential backoff.
         *
         * Default: 1000 milliseconds.
         */
        var initialDelayMs: Long = 1000,
    )
}