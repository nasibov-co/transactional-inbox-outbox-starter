package com.fnasibov.transactional.inbox.outbox.starter.r2dbc.domain

import com.fnasibov.transactional.inbox.outbox.starter.r2dbc.api.FetchBatchStrategy
import com.fnasibov.transactional.inbox.outbox.starter.r2dbc.api.model.Event
import com.fnasibov.transactional.inbox.outbox.starter.r2dbc.api.model.EventStatus
import com.fnasibov.transactional.inbox.outbox.starter.r2dbc.configuration.TransactionalProperties
import kotlinx.coroutines.reactive.awaitSingle
import kotlinx.coroutines.reactor.awaitSingle
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.data.relational.core.mapping.Table
import org.springframework.data.relational.core.query.Criteria.where
import org.springframework.data.relational.core.query.Query
import org.springframework.transaction.reactive.TransactionalOperator
import reactor.core.publisher.Mono
import java.time.ZonedDateTime
import java.util.*

/**
 * Default implementation of [EventRepository] responsible for managing
 * event persistence and batch polling operations.
 *
 * The repository provides a generic implementation for:
 * - fetching event batches with row-level locking
 * - saving events
 * - updating processing statuses
 * - retry and dead-letter handling
 *
 * Custom batch fetching behavior can be provided per event type through
 * [FetchBatchStrategy]. If no strategy is registered for an event type,
 * the repository falls back to the default polling implementation.
 *
 * The default fetch implementation:
 * - selects events eligible for processing
 * - locks rows using `FOR UPDATE SKIP LOCKED`
 * - marks selected events as `PROCESSING`
 * - returns the loaded entities in a single transaction
 */
class BaseEventRepository(
    private val template: R2dbcEntityTemplate,
    private val properties: TransactionalProperties,
    private val transactionalOperator: TransactionalOperator,
    private val strategiesByEventType: Map<Class<out Event>, FetchBatchStrategy<out Event>>
) : EventRepository {

    /**
     * Fetches a batch of events for processing.
     *
     * If a custom [FetchBatchStrategy] is registered for the provided event type,
     * the strategy implementation is used. Otherwise, the default batch polling
     * mechanism is executed.
     *
     * The default implementation selects events with eligible statuses,
     * applies backoff logic based on `last_attempt_at`,
     * locks rows using `FOR UPDATE SKIP LOCKED`,
     * and marks fetched events as `PROCESSING`.
     *
     * @param eventType event class to fetch
     * @return list of locked events ready for processing
     */
    @Suppress("UNCHECKED_CAST")
    override suspend fun <E : Event> fetchBatch(eventType: Class<E>): List<E> {
        val strategy = strategiesByEventType[eventType] as? FetchBatchStrategy<E>
        if (strategy != null) {
            return strategy.fetchBatch()
        }
        return defaultFetchBatch(eventType)
    }

    /**
     * Default transactional batch polling implementation.
     *
     * This method:
     * - selects candidate event ids
     * - locks rows to prevent concurrent processing
     * - updates event status to `PROCESSING`
     * - loads and returns updated entities
     *
     * Events are filtered using retry backoff configuration
     * and ordered by creation time.
     */
    private suspend fun <E : Event> defaultFetchBatch(eventType: Class<E>): List<E> {
        val now = ZonedDateTime.now()
        val backoffTime = now.minusSeconds(properties.polling.activeIntervalMs.seconds)

        val tableName = getTableName(eventType)
        val batchSize = properties.polling.batchSize

        val selectIdsSql = """
        SELECT id
        FROM $tableName
        WHERE status in (:statuses)
          AND (
              last_attempt_at IS NULL
              OR last_attempt_at < :backoffTime
          )
        ORDER BY created_at ASC
        LIMIT :limit
        FOR UPDATE SKIP LOCKED
    """.trimIndent()

        val updateStatusSql = """
        UPDATE $tableName
        SET status = 'PROCESSING',
            last_attempt_at = :now,
            updated_at = :now
        WHERE id IN (:ids)
    """.trimIndent()

        return transactionalOperator.execute {
            template.databaseClient.sql(selectIdsSql)
                .bind("statuses", listOf(EventStatus.PENDING.name, EventStatus.PROCESSING.name, EventStatus.FAILED.name))
                .bind("backoffTime", backoffTime)
                .bind("limit", batchSize)
                .map { row, _ ->
                    row.get("id", UUID::class.java)!!
                }
                .all()
                .collectList()
                .flatMap { ids ->

                    if (ids.isEmpty()) {
                        return@flatMap Mono.just(emptyList())
                    }

                    template.databaseClient.sql(updateStatusSql)
                        .bind("now", now)
                        .bind("ids", ids)
                        .fetch()
                        .rowsUpdated()
                        .thenMany(
                            template.select(
                                Query.query(where("id").`in`(ids)),
                                eventType
                            )
                        )
                        .collectList()
                }
        }.awaitSingle()
    }

    /**
     * Marks the event as successfully processed.
     *
     * Updates the event status to `PROCESSED`
     * and refreshes the `updated_at` timestamp.
     *
     * @param event processed event
     */
    override suspend fun <E : Event> markAsProcessed(
        event: E
    ) {
        val tableName = getTableName(event.javaClass)
        val sql = """
            UPDATE $tableName
            SET status = :status,
                updated_at = :updatedAt
            WHERE id = :id
        """.trimIndent()

        template.databaseClient.sql(sql)
            .bind("status", EventStatus.PROCESSED.name)
            .bind("updatedAt", ZonedDateTime.now())
            .bind("id", event.id)
            .fetch()
            .rowsUpdated()
            .awaitSingle()
    }

    /**
     * Moves the event to dead-letter state.
     *
     * Updates the event status to `DEAD_LETTER`
     * and refreshes the `updated_at` timestamp.
     *
     * @param event failed event
     */
    override suspend fun <E : Event> markAsDeadLetter(
        event: E
    ) {
        val tableName = getTableName(event.javaClass)

        val sql = """
            UPDATE $tableName
            SET status = :status,
                updated_at = :updatedAt
            WHERE id = :id
        """.trimIndent()

        template.databaseClient.sql(sql)
            .bind("status", EventStatus.DEAD_LETTER.name)
            .bind("updatedAt", ZonedDateTime.now())
            .bind("id", event.id)
            .fetch()
            .rowsUpdated()
            .awaitSingle()
    }

    /**
     * Handles failed event processing by updating retry state and status.
     *
     * Increments retry counter and updates event status based on retry policy.
     * If the retry limit is exceeded, the event is moved to `DEAD_LETTER`.
     *
     * Also updates `last_attempt_at` and `updated_at` timestamps.
     *
     * @param event event that failed during processing
     * @return resulting event status after applying failure handling logic
     */
    override suspend fun <E : Event> markAsFailed(event: E): EventStatus {
        val tableName = getTableName(event.javaClass)

        val nextRetryCount = event.retryCount + 1

        val nextStatus =
            if (nextRetryCount < properties.retry.maxImmediateAttempts) {
                EventStatus.FAILED
            } else {
                EventStatus.DEAD_LETTER
            }

        val sql = """
            UPDATE $tableName
            SET status = :status,
                retry_count = :retryCount,
                last_attempt_at = NOW(),
                updated_at = NOW()
            WHERE id = :id
        """.trimIndent()

        template.databaseClient.sql(sql)
            .bind("status", nextStatus.name)
            .bind("retryCount", nextRetryCount)
            .bind("id", event.id)
            .fetch()
            .rowsUpdated()
            .awaitSingle()

        return nextStatus
    }

    /**
     * Resolves database table name for the provided event type.
     *
     * The event class must be annotated with [Table]
     * and contain a non-empty table name.
     *
     * @throws IllegalStateException if table mapping is missing
     */
    private fun <E : Event> getTableName(
        eventType: Class<E>
    ): String {

        val annotation = eventType.getAnnotation(
            Table::class.java
        ) ?: error("Event ${eventType.name} must be annotated with @Table")

        return annotation.value.takeIf { it.isNotBlank() }
            ?: error("@Table value must not be empty for event ${eventType.name}")
    }
}