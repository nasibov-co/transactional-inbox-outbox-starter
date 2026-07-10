package com.fnasibov.transactional.inbox.outbox.starter.r2dbc.configuration

import com.fnasibov.transactional.inbox.outbox.starter.r2dbc.api.model.Event
import jakarta.validation.Valid
import jakarta.validation.constraints.DecimalMin
import jakarta.validation.constraints.Min
import jakarta.validation.constraints.NotBlank
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.validation.annotation.Validated
import java.time.Duration

/**
 * Configuration properties for transactional inbox/outbox processing.
 *
 * Defines polling, processing, and retry behavior
 * used by the event processing infrastructure.
 */
@Validated
@ConfigurationProperties(prefix = "transactional")
data class TransactionalProperties(

    /**
     * Enables or disables transactional inbox/outbox processing.
     */
    var enabled: Boolean = false,

    /**
     * Polling configuration.
     */
    @field:Valid
    var polling: Polling = Polling(),

    /**
     * Event processing configuration.
     */
    @field:Valid
    var processing: Processing = Processing(),

    /**
     * Retry configuration.
     */
    @field:Valid
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
        var maxIdleInterval: Duration = Duration.ofSeconds(30),

        /**
         * Polling interval while events are actively being processed.
         *
         * Default: 100 milliseconds.
         */
        var activeInterval: Duration = Duration.ofMillis(100),

        /**
         * Maximum number of events fetched in a single batch.
         *
         * Default: 15.
         */
        @field:Min(1)
        var batchSize: Int = 15,

        /**
         * Internal channel capacity used for buffering fetched events.
         *
         * Default: 25.
         */
        @field:Min(1)
        var channelCapacity: Int = 25,

        /**
         * Time after which an event left in PROCESSING can be picked up again.
         *
         * Default: 5 minutes.
         */
        var processingStaleTimeout: Duration = Duration.ofMinutes(5)
    )

    /**
     * Event processing execution settings.
     */
    data class Processing(

        /**
         * Default number of concurrent event processing workers per event type.
         *
         * Default: 5.
         */
        @field:Min(1)
        var concurrency: Int = 5,

        /**
         * Event type specific processing settings.
         */
        @field:Valid
        var eventTypes: List<EventTypeProcessing> = emptyList(),

        /**
         * Maximum time to wait for workers to drain already fetched events
         * during application shutdown.
         *
         * Default: 30 seconds.
         */
        var shutdownTimeout: Duration = Duration.ofSeconds(30)
    ) {

        fun concurrencyFor(eventType: Class<out Event>): Int =
            eventTypes.firstOrNull { it.matches(eventType) }?.concurrency
                ?: concurrency

        fun retryFor(
            eventType: Class<out Event>,
            defaultRetry: Retry
        ): ResolvedRetry {
            val eventRetry = eventTypes.firstOrNull { it.matches(eventType) }?.retry

            return ResolvedRetry(
                maxAttempts = eventRetry?.maxAttempts ?: defaultRetry.maxAttempts,
                initialDelay = eventRetry?.initialDelay ?: defaultRetry.initialDelay,
                multiplier = eventRetry?.multiplier ?: defaultRetry.multiplier,
                maxDelay = eventRetry?.maxDelay ?: defaultRetry.maxDelay
            )
        }
    }

    /**
     * Processing settings for a specific event type.
     */
    data class EventTypeProcessing(

        /**
         * Fully qualified or simple event class name.
         */
        @field:NotBlank
        var eventType: String = "",

        /**
         * Number of concurrent workers for the event type.
         */
        @field:Min(1)
        var concurrency: Int? = null,

        /**
         * Event type specific retry settings.
         */
        @field:Valid
        var retry: EventTypeRetry? = null
    ) {

        fun matches(type: Class<out Event>): Boolean =
            eventType == type.name ||
                eventType == type.canonicalName ||
                eventType == type.simpleName
    }

    /**
     * Retry override for a specific event type.
     */
    data class EventTypeRetry(

        /**
         * Maximum number of retry attempts before moving
         * an event to dead-letter state.
         */
        @field:Min(1)
        var maxAttempts: Int? = null,

        /**
         * Initial retry delay used.
         */
        var initialDelay: Duration? = null,

        /**
         * Multiplier applied to retry delay after each failed attempt.
         */
        @field:DecimalMin("1.0")
        var multiplier: Double? = null,

        /**
         * Maximum retry delay.
         */
        var maxDelay: Duration? = null
    )

    /**
     * Fully resolved retry behavior for an event type.
     */
    data class ResolvedRetry(
        val maxAttempts: Int,
        val initialDelay: Duration,
        val multiplier: Double,
        val maxDelay: Duration
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
        @field:Min(1)
        var maxAttempts: Int = 3,

        /**
         * Initial retry delay used.
         *
         * Default: 1000 milliseconds.
         */
        var initialDelay: Duration = Duration.ofSeconds(1),

        /**
         * Multiplier applied to retry delay after each failed attempt.
         *
         * Default: 2.0.
         */
        @field:DecimalMin("1.0")
        var multiplier: Double = 2.0,

        /**
         * Maximum retry delay.
         *
         * Default: 1 minute.
         */
        var maxDelay: Duration = Duration.ofMinutes(1),
    )
}
