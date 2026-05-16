package com.fnasibov.transactional.inbox.outbox.starter.r2dbc.configuration

import java.time.Duration


data class TransactionalProperties(

    /**
     * General settings.
     */
    var enabled: Boolean = false,

    /**
     * Polling configuration.
     */
    var polling: Polling = Polling(),

    var processing: Processing = Processing(),

    /**
     * Retry configuration for immediate retries within the dispatcher.
     */
    var retry: Retry = Retry()

){

    data class Polling(
        /**
         * Maximum intervar between polling cycles when no events are found.
         * Default: 30 seconds.
         */
        var maxIdleIntervalMs: Duration = Duration.ofMillis(30000),

        /**
         * Intervar between polling cycles when events were processed.
         * Default: 100 milliseconds.
         */
        var activeIntervalMs: Duration = Duration.ofMillis(100),

        /**
         * Maximum number of events to fetch in a single batch.
         * Default: 10.
         */
        var batchSize: Int = 15,

        /**
         * Maximum concurrency for processing events in a batch.
         * Default: 5.
         */
        var maxConcurrency: Int = 5,

        var channelCapacity: Int = 25
    )

    data class Processing(
        var concurrency: Int = 5
    )
    data class Retry(
        /**
         * Maximum number of immediate retry attempts for transient errors.
         * Default: 3.
         */
        var maxImmediateAttempts: Int = 3,

        /**
         * Initial delay for exponential backoff in milliseconds.
         * Default: 1000 ms (1 second).
         */
        var initialDelayMs: Long = 1000,
    )
}
