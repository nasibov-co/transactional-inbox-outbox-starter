package com.fnasibov.transactional.inbox.outbox.jdbc

import com.fnasibov.transactional.inbox.outbox.core.api.FetchBatchStrategy
import com.fnasibov.transactional.inbox.outbox.core.api.model.Event
import com.fnasibov.transactional.inbox.outbox.core.api.model.EventStatus
import com.fnasibov.transactional.inbox.outbox.core.configuration.TransactionalProperties
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
    private val strategiesByEventType: Map<Class<out Event>, FetchBatchStrategy<out Event>>
) : EventRepository {

    @Suppress("UNCHECKED_CAST")
    override suspend fun <E : Event> fetchBatch(eventType: Class<E>): List<E> {
        val strategy = strategiesByEventType[eventType] as? FetchBatchStrategy<E>
        return strategy?.fetchBatch() ?: runInterruptible(Dispatchers.IO) {
            defaultFetchBatch(eventType)
        }
    }

    private fun <E : Event> defaultFetchBatch(eventType: Class<E>): List<E> {
        val now = ZonedDateTime.now()
        val tableName = getTableName(eventType)
        val selectParameters = MapSqlParameterSource()
            .addValue("pendingStatus", EventStatus.PENDING.name)
            .addValue("processingStatus", EventStatus.PROCESSING.name)
            .addValue("failedStatus", EventStatus.FAILED.name)
            .addValue(
                "processingStaleBefore",
                EventPollingQueries.processingStaleBefore(now, properties).toOffsetDateTime()
            )
            .addValue("now", now.toOffsetDateTime())
            .addValue("limit", properties.polling.batchSize)

        return transactionTemplate.execute {
            val ids = jdbc.queryForList(
                EventPollingQueries.selectIdsSql(tableName),
                selectParameters,
                UUID::class.java
            )
            if (ids.isEmpty()) {
                return@execute emptyList()
            }

            jdbc.update(
                EventPollingQueries.updateStatusSql(tableName),
                MapSqlParameterSource()
                    .addValue("processingStatus", EventStatus.PROCESSING.name)
                    .addValue("now", now.toOffsetDateTime())
                    .addValue("ids", ids)
            )
            aggregates.findAllById(ids, eventType).toList()
        } ?: emptyList()
    }

    override suspend fun <E : Event> markAsProcessed(event: E) =
        updateStatus(event, EventStatus.PROCESSED)

    override suspend fun <E : Event> markAsDeadLetter(event: E) =
        updateStatus(event, EventStatus.DEAD_LETTER)

    override suspend fun <E : Event> markAsFailed(event: E): EventStatus =
        runInterruptible(Dispatchers.IO) {
            val retry = properties.processing.retryFor(event.javaClass, properties.retry)
            val nextRetryCount = event.retryCount + 1
            val now = ZonedDateTime.now()
            val nextStatus = if (nextRetryCount < retry.maxAttempts) {
                EventStatus.FAILED
            } else {
                EventStatus.DEAD_LETTER
            }
            val parameters = MapSqlParameterSource()
                .addValue("status", nextStatus.name)
                .addValue("retryCount", nextRetryCount)
                .addValue("now", now)
                .addValue("id", event.id)

            val nextRetryUpdate = if (nextStatus == EventStatus.FAILED) {
                parameters.addValue(
                    "nextRetryAt",
                    now.plus(nextRetryDelay(nextRetryCount, retry)).toOffsetDateTime()
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
                parameters
            )
            nextStatus
        }

    private suspend fun <E : Event> updateStatus(event: E, status: EventStatus) {
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
                    .addValue("id", event.id)
            )
        }
    }

    private fun nextRetryDelay(
        retryCount: Int,
        retry: TransactionalProperties.ResolvedRetry
    ): Duration {
        val multiplier = retry.multiplier.pow((retryCount - 1).coerceAtLeast(0))
        val delayMillis = (retry.initialDelay.toMillis() * multiplier).toLong()
        return Duration.ofMillis(delayMillis).coerceAtMost(retry.maxDelay)
    }

    private fun <E : Event> getTableName(eventType: Class<E>): String {
        val annotation = eventType.getAnnotation(Table::class.java)
            ?: error("Event ${eventType.name} must be annotated with @Table")
        return annotation.value.takeIf { it.isNotBlank() }
            ?: error("@Table value must not be empty for event ${eventType.name}")
    }
}
