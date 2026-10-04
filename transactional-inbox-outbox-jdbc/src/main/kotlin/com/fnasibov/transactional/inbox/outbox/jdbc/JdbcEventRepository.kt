package com.fnasibov.transactional.inbox.outbox.jdbc

import com.fnasibov.transactional.inbox.outbox.core.api.FetchBatchStrategy
import com.fnasibov.transactional.inbox.outbox.core.api.model.Event
import com.fnasibov.transactional.inbox.outbox.core.api.model.EventStatus
import com.fnasibov.transactional.inbox.outbox.core.configuration.TransactionalProperties
import com.fnasibov.transactional.inbox.outbox.core.domain.BatchKeyDescriptor
import com.fnasibov.transactional.inbox.outbox.core.domain.BatchKeySupport
import com.fnasibov.transactional.inbox.outbox.core.domain.EventRepository
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runInterruptible
import org.springframework.data.jdbc.core.JdbcAggregateOperations
import org.springframework.data.relational.core.mapping.Table
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations
import org.springframework.transaction.support.TransactionTemplate
import java.time.Duration
import java.time.ZonedDateTime
import java.util.UUID
import kotlin.math.pow

/** JDBC implementation of the event persistence contract. */
class JdbcEventRepository(
    private val jdbc: NamedParameterJdbcOperations,
    private val aggregates: JdbcAggregateOperations,
    private val transactionTemplate: TransactionTemplate,
    private val properties: TransactionalProperties,
    private val strategiesByEventType: Map<Class<out Event>, FetchBatchStrategy<out Event>>,
) : EventRepository {
    @Suppress("UNCHECKED_CAST")
    override suspend fun <E : Event> fetchBatch(eventType: Class<E>): List<E> {
        val strategy = strategiesByEventType[eventType] as? FetchBatchStrategy<E>
        return strategy?.fetchBatch() ?: runInterruptible(Dispatchers.IO) {
            val batchKey = BatchKeySupport.batchKeyDescriptor(eventType)
            if (batchKey != null) {
                batchKeyFetchBatch(eventType, batchKey)
            } else {
                defaultFetchBatch(eventType)
            }
        }
    }

    private fun <E : Event> defaultFetchBatch(eventType: Class<E>): List<E> {
        val now = ZonedDateTime.now()
        val tableName = getTableName(eventType)
        val selectParameters =
            MapSqlParameterSource()
                .addValue("pendingStatus", EventStatus.PENDING.name)
                .addValue("processingStatus", EventStatus.PROCESSING.name)
                .addValue("failedStatus", EventStatus.FAILED.name)
                .addValue(
                    "processingStaleBefore",
                    EventPollingQueries.processingStaleBefore(now, properties).toOffsetDateTime(),
                ).addValue("now", now.toOffsetDateTime())
                .addValue("limit", properties.polling.batchSize)

        return transactionTemplate.execute {
            val ids =
                jdbc.queryForList(
                    EventPollingQueries.selectIdsSql(tableName),
                    selectParameters,
                    UUID::class.java,
                )
            if (ids.isEmpty()) {
                return@execute emptyList()
            }

            jdbc.update(
                EventPollingQueries.updateStatusSql(tableName),
                MapSqlParameterSource()
                    .addValue("processingStatus", EventStatus.PROCESSING.name)
                    .addValue("now", now.toOffsetDateTime())
                    .addValue("ids", ids),
            )
            aggregates.findAllById(ids, eventType).toList()
        } ?: emptyList()
    }

    /**
     * Batch-key fetch implementation for event models annotated with `@BatchKey`.
     *
     * Within a single transaction this method:
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
    private fun <E : Event> batchKeyFetchBatch(
        eventType: Class<E>,
        batchKey: BatchKeyDescriptor,
    ): List<E> {
        val now = ZonedDateTime.now()
        val tableName = getTableName(eventType)

        fun pollingParameters(): MapSqlParameterSource =
            MapSqlParameterSource()
                .addValue("pendingStatus", EventStatus.PENDING.name)
                .addValue("processingStatus", EventStatus.PROCESSING.name)
                .addValue("failedStatus", EventStatus.FAILED.name)
                .addValue("processedStatus", EventStatus.PROCESSED.name)
                .addValue("deadLetterStatus", EventStatus.DEAD_LETTER.name)
                .addValue(
                    "processingStaleBefore",
                    EventPollingQueries.processingStaleBefore(now, properties).toOffsetDateTime(),
                ).addValue("now", now.toOffsetDateTime())

        return transactionTemplate.execute {
            val keyValues =
                jdbc.queryForList(
                    EventPollingQueries.selectBatchKeySql(tableName, batchKey.columnName),
                    pollingParameters(),
                    Any::class.java,
                )
            if (keyValues.isEmpty()) {
                return@execute emptyList<E>()
            }
            val pickedKey = keyValues.first()
            val keyIsNull = pickedKey == null

            fun keyParameters(): MapSqlParameterSource =
                if (keyIsNull) {
                    pollingParameters()
                } else {
                    pollingParameters().addValue("batchKey", pickedKey)
                }

            jdbc.queryForList(
                EventPollingQueries.lockBatchKeyGroupSql(
                    tableName,
                    batchKey.columnName,
                    keyIsNull,
                ),
                keyParameters(),
                UUID::class.java,
            )

            val ids =
                jdbc.queryForList(
                    EventPollingQueries.selectIdsForBatchKeySql(
                        tableName,
                        batchKey.columnName,
                        keyIsNull,
                    ),
                    keyParameters().addValue("limit", properties.polling.batchSize),
                    UUID::class.java,
                )
            if (ids.isEmpty()) {
                return@execute emptyList<E>()
            }

            val events = aggregates.findAllById(ids, eventType).toList()
            val batch =
                BatchKeySupport.selectKeyGroup(
                    events,
                    properties.polling.batchSize,
                    batchKey,
                )
            if (batch.isEmpty()) {
                return@execute emptyList<E>()
            }

            jdbc.update(
                EventPollingQueries.updateStatusSql(tableName),
                MapSqlParameterSource()
                    .addValue("processingStatus", EventStatus.PROCESSING.name)
                    .addValue("now", now.toOffsetDateTime())
                    .addValue("ids", batch.map { it.id }),
            )
            batch
        } ?: emptyList()
    }

    override suspend fun <E : Event> markAsProcessed(event: E) = updateStatus(event, EventStatus.PROCESSED)

    override suspend fun <E : Event> markAsDeadLetter(event: E) = updateStatus(event, EventStatus.DEAD_LETTER)

    override suspend fun <E : Event> markAsFailed(event: E): EventStatus =
        runInterruptible(Dispatchers.IO) {
            val retry = properties.processing.retryFor(event.javaClass, properties.retry)
            val nextRetryCount = event.retryCount + 1
            val now = ZonedDateTime.now()
            val nextStatus =
                if (nextRetryCount < retry.maxAttempts) {
                    EventStatus.FAILED
                } else {
                    EventStatus.DEAD_LETTER
                }
            val parameters =
                MapSqlParameterSource()
                    .addValue("status", nextStatus.name)
                    .addValue("retryCount", nextRetryCount)
                    .addValue("now", now.toOffsetDateTime())
                    .addValue("id", event.id)

            val nextRetryUpdate =
                if (nextStatus == EventStatus.FAILED) {
                    parameters.addValue(
                        "nextRetryAt",
                        now.plus(nextRetryDelay(nextRetryCount, retry)).toOffsetDateTime(),
                    )
                    "next_retry_at = :nextRetryAt"
                } else {
                    "next_retry_at = NULL"
                }

            jdbc.update(
                """
                UPDATE ${getTableName(event.javaClass)}
                SET status = :status,
                    retry_count = :retryCount,
                    last_attempt_at = :now,
                    updated_at = :now,
                    $nextRetryUpdate
                WHERE id = :id
                """.trimIndent(),
                parameters,
            )
            nextStatus
        }

    private suspend fun <E : Event> updateStatus(
        event: E,
        status: EventStatus,
    ) {
        runInterruptible(Dispatchers.IO) {
            jdbc.update(
                """
                UPDATE ${getTableName(event.javaClass)}
                SET status = :status,
                    updated_at = :updatedAt,
                    next_retry_at = NULL
                WHERE id = :id
                """.trimIndent(),
                MapSqlParameterSource()
                    .addValue("status", status.name)
                    .addValue("updatedAt", ZonedDateTime.now().toOffsetDateTime())
                    .addValue("id", event.id),
            )
        }
    }

    private fun nextRetryDelay(
        retryCount: Int,
        retry: TransactionalProperties.ResolvedRetry,
    ): Duration {
        val multiplier = retry.multiplier.pow((retryCount - 1).coerceAtLeast(0))
        val delayMillis = (retry.initialDelay.toMillis() * multiplier).toLong()
        return Duration.ofMillis(delayMillis).coerceAtMost(retry.maxDelay)
    }

    private fun <E : Event> getTableName(eventType: Class<E>): String {
        val annotation =
            eventType.getAnnotation(Table::class.java)
                ?: error("Event ${eventType.name} must be annotated with @Table")
        return annotation.value.takeIf { it.isNotBlank() }
            ?: error("@Table value must not be empty for event ${eventType.name}")
    }
}
