package com.fnasibov.transactional.inbox.outbox.demo.jdbc

import com.fnasibov.transactional.inbox.outbox.core.api.BlockingFetchBatchStrategy
import com.fnasibov.transactional.inbox.outbox.core.api.model.EventStatus
import com.fnasibov.transactional.inbox.outbox.core.configuration.TransactionalProperties
import org.springframework.data.jdbc.core.JdbcAggregateOperations
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations
import org.springframework.stereotype.Component
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.support.TransactionTemplate
import java.time.OffsetDateTime
import java.util.UUID

@Component
class PriorityJdbcDemoEventFetchBatchStrategy(
    private val jdbc: NamedParameterJdbcOperations,
    private val aggregates: JdbcAggregateOperations,
    transactionManager: PlatformTransactionManager,
    private val properties: TransactionalProperties
) : BlockingFetchBatchStrategy<JdbcDemoEvent> {

    private val transactionTemplate = TransactionTemplate(transactionManager)

    override val eventType: Class<JdbcDemoEvent> =
        JdbcDemoEvent::class.java

    override fun fetchBatch(): List<JdbcDemoEvent> =
        transactionTemplate.execute {
            val now = OffsetDateTime.now()
            val ids = jdbc.queryForList(
                SELECT_IDS_SQL,
                MapSqlParameterSource()
                    .addValue("pendingStatus", EventStatus.PENDING.name)
                    .addValue("processingStatus", EventStatus.PROCESSING.name)
                    .addValue("failedStatus", EventStatus.FAILED.name)
                    .addValue(
                        "processingStaleBefore",
                        now.minus(properties.polling.processingStaleTimeout)
                    )
                    .addValue("now", now)
                    .addValue("limit", properties.polling.batchSize),
                UUID::class.java
            )
            if (ids.isEmpty()) {
                return@execute emptyList()
            }

            jdbc.update(
                UPDATE_STATUS_SQL,
                MapSqlParameterSource()
                    .addValue("status", EventStatus.PROCESSING.name)
                    .addValue("now", now)
                    .addValue("ids", ids)
            )
            aggregates.findAllById(ids, JdbcDemoEvent::class.java).toList()
        } ?: emptyList()

    private companion object {
        val SELECT_IDS_SQL = """
            SELECT id
            FROM jdbc_demo_events
            WHERE status = :pendingStatus
            OR (
                status = :processingStatus
                AND last_attempt_at IS NOT NULL
                AND last_attempt_at < :processingStaleBefore
            )
            OR (
                status = :failedStatus
                AND (
                    next_retry_at IS NULL
                    OR next_retry_at <= :now
                )
            )
            ORDER BY priority DESC, created_at ASC
            LIMIT :limit
            FOR UPDATE SKIP LOCKED
        """.trimIndent()

        val UPDATE_STATUS_SQL = """
            UPDATE jdbc_demo_events
            SET status = :status,
                last_attempt_at = :now,
                updated_at = :now,
                next_retry_at = NULL
            WHERE id IN (:ids)
        """.trimIndent()
    }
}
