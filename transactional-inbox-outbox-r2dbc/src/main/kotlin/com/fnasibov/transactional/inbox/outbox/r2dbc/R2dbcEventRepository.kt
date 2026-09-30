package com.fnasibov.transactional.inbox.outbox.r2dbc

import com.fnasibov.transactional.inbox.outbox.core.api.FetchBatchStrategy
import com.fnasibov.transactional.inbox.outbox.core.api.model.Event
import com.fnasibov.transactional.inbox.outbox.core.api.model.EventStatus
import com.fnasibov.transactional.inbox.outbox.core.configuration.TransactionalProperties
import com.fnasibov.transactional.inbox.outbox.core.domain.BatchKeyDescriptor
import com.fnasibov.transactional.inbox.outbox.core.domain.BatchKeySupport
import com.fnasibov.transactional.inbox.outbox.core.domain.EventRepository
import kotlinx.coroutines.reactive.awaitSingle
import kotlinx.coroutines.reactor.awaitSingle
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.data.relational.core.mapping.Table
import org.springframework.data.relational.core.query.Criteria.where
import org.springframework.data.relational.core.query.Query
import org.springframework.r2dbc.core.bind
import org.springframework.transaction.reactive.TransactionalOperator
import reactor.core.publisher.Mono
import java.time.Duration
import java.time.ZonedDateTime
import java.util.*
import kotlin.math.pow

/**
 * Identity marker mapped in place of a SQL `NULL` batch key value.
 *
 * The row mapper of the reactive client cannot produce nullable results, so a
 * `NULL` key column is mapped to this marker and translated back into the
 * `IS NULL` query shape when the key group is locked and selected.
 */
internal val nullBatchKeyMarker = Any()

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
class R2dbcEventRepository(
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
        val batchKey = BatchKeySupport.batchKeyDescriptor(eventType)
        return if (batchKey != null) {
            batchKeyFetchBatch(eventType, batchKey)
        } else {
            defaultFetchBatch(eventType)
        }
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
        val processingStaleBefore = EventPollingQueries.processingStaleBefore(
            now,
            properties
        )

        val tableName = getTableName(eventType)
        val batchSize = properties.polling.batchSize

        val selectIdsSql = EventPollingQueries.selectIdsSql(tableName)
        val updateStatusSql = EventPollingQueries.updateStatusSql(tableName)

        return transactionalOperator.execute {
            template.databaseClient.sql(selectIdsSql)
                .bind("pendingStatus", EventStatus.PENDING.name)
                .bind("processingStatus", EventStatus.PROCESSING.name)
                .bind("failedStatus", EventStatus.FAILED.name)
                .bind("processingStaleBefore", processingStaleBefore)
                .bind("now", now)
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
     * Batch-key fetch implementation for event models annotated with `@BatchKey`.
     *
     * Within a single reactive transaction this method:
     * - picks the batch key of the oldest event that is eligible and not claimed
     *   by another instance,
     * - locks the non-terminal rows of that key group with `FOR UPDATE` so that
     *   concurrent fetch transactions for the same key are serialized,
     * - re-selects up to `batchSize` eligible events of the key, refusing the
     *   fetch while the key holds an active claim,
     * - marks the returned events as `PROCESSING`, and
     * - returns a single-key batch capped at `batchSize`.
     *
     * The committed `PROCESSING` rows act as a durable claim on the key that
     * outlives the fetch transaction: other fetches skip keys with such rows
     * until the whole batch leaves `PROCESSING` or its `last_attempt_at` grows
     * older than the processing stale timeout (crash recovery, lease expiry).
     */
    private suspend fun <E : Event> batchKeyFetchBatch(
        eventType: Class<E>,
        batchKey: BatchKeyDescriptor
    ): List<E> {
        val now = ZonedDateTime.now()
        val processingStaleBefore = EventPollingQueries.processingStaleBefore(now, properties)
        val tableName = getTableName(eventType)
        val batchSize = properties.polling.batchSize

        return transactionalOperator.execute {
            template.databaseClient.sql(
                EventPollingQueries.selectBatchKeySql(tableName, batchKey.columnName)
            )
                .bind("pendingStatus", EventStatus.PENDING.name)
                .bind("processingStatus", EventStatus.PROCESSING.name)
                .bind("failedStatus", EventStatus.FAILED.name)
                .bind("processingStaleBefore", processingStaleBefore)
                .bind("now", now)
                .map { row, _ -> row.get(batchKey.columnName) ?: nullBatchKeyMarker }
                .all()
                .collectList()
                .flatMap { keyValues ->

                    if (keyValues.isEmpty()) {
                        return@flatMap Mono.just(emptyList<E>())
                    }

                    val pickedKey = keyValues.first()
                    val keyIsNull = pickedKey === nullBatchKeyMarker

                    var lock = template.databaseClient.sql(
                        EventPollingQueries.lockBatchKeyGroupSql(
                            tableName,
                            batchKey.columnName,
                            keyIsNull
                        )
                    )
                        .bind("processedStatus", EventStatus.PROCESSED.name)
                        .bind("deadLetterStatus", EventStatus.DEAD_LETTER.name)
                    if (!keyIsNull) {
                        lock = lock.bind("batchKey", pickedKey)
                    }

                    lock.map { row, _ -> row.get("id", UUID::class.java)!! }
                        .all()
                        .collectList()
                        .flatMap {

                            var select = template.databaseClient.sql(
                                EventPollingQueries.selectIdsForBatchKeySql(
                                    tableName,
                                    batchKey.columnName,
                                    keyIsNull
                                )
                            )
                                .bind("pendingStatus", EventStatus.PENDING.name)
                                .bind("processingStatus", EventStatus.PROCESSING.name)
                                .bind("failedStatus", EventStatus.FAILED.name)
                                .bind("processingStaleBefore", processingStaleBefore)
                                .bind("now", now)
                                .bind("limit", batchSize)
                            if (!keyIsNull) {
                                select = select.bind("batchKey", pickedKey)
                            }

                            select.map { row, _ -> row.get("id", UUID::class.java)!! }
                                .all()
                                .collectList()
                                .flatMap { ids ->

                                    if (ids.isEmpty()) {
                                        return@flatMap Mono.just(emptyList<E>())
                                    }

                                    template.select(
                                        Query.query(where("id").`in`(ids)),
                                        eventType
                                    )
                                        .collectList()
                                        .flatMap { events ->
                                            val batch = BatchKeySupport.selectKeyGroup(
                                                events,
                                                batchSize,
                                                batchKey
                                            )
                                            if (batch.isEmpty()) {
                                                Mono.just(emptyList<E>())
                                            } else {
                                                template.databaseClient.sql(
                                                    EventPollingQueries.updateStatusSql(tableName)
                                                )
                                                    .bind("now", now)
                                                    .bind("ids", batch.map { it.id })
                                                    .fetch()
                                                    .rowsUpdated()
                                                    .then(Mono.just(batch))
                                            }
                                        }
                                }
                        }
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
                updated_at = :updatedAt,
                next_retry_at = NULL
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
                updated_at = :updatedAt,
                next_retry_at = NULL
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
        val retry = properties.processing.retryFor(event.javaClass, properties.retry)

        val nextRetryCount = event.retryCount + 1
        val now = ZonedDateTime.now()
        val nextRetryAt = now.plus(nextRetryDelay(nextRetryCount, retry))

        val nextStatus =
            if (nextRetryCount < retry.maxAttempts) {
                EventStatus.FAILED
            } else {
                EventStatus.DEAD_LETTER
            }

        val nextRetryAtUpdate =
            if (nextStatus == EventStatus.FAILED) {
                "next_retry_at = :nextRetryAt"
            } else {
                "next_retry_at = NULL"
            }

        val sql = """
            UPDATE $tableName
            SET status = :status,
                retry_count = :retryCount,
                last_attempt_at = :now,
                updated_at = :now,
                $nextRetryAtUpdate
            WHERE id = :id
        """.trimIndent()

        var statement = template.databaseClient.sql(sql)
            .bind("status", nextStatus.name)
            .bind("retryCount", nextRetryCount)
            .bind("now", now)
            .bind("id", event.id)

        if (nextStatus == EventStatus.FAILED) {
            statement = statement.bind("nextRetryAt", nextRetryAt)
        }

        statement
            .fetch()
            .rowsUpdated()
            .awaitSingle()

        return nextStatus
    }

    private fun nextRetryDelay(
        retryCount: Int,
        retry: TransactionalProperties.ResolvedRetry
    ): Duration {
        val multiplier = retry.multiplier.pow((retryCount - 1).coerceAtLeast(0))
        val delayMillis = (retry.initialDelay.toMillis() * multiplier).toLong()
        return Duration.ofMillis(delayMillis).coerceAtMost(retry.maxDelay)
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
