package com.fnasibov.transactional.inbox.outbox.r2dbc

import com.fnasibov.transactional.inbox.outbox.core.configuration.TransactionalProperties
import java.time.ZonedDateTime

internal object EventPollingQueries {

    fun selectIdsSql(tableName: String): String = """
        SELECT id
        FROM $tableName
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
        ORDER BY created_at ASC
        LIMIT :limit
        FOR UPDATE SKIP LOCKED
    """.trimIndent()

    fun updateStatusSql(tableName: String): String = """
        UPDATE $tableName
        SET status = 'PROCESSING',
            last_attempt_at = :now,
            updated_at = :now,
            next_retry_at = NULL
        WHERE id IN (:ids)
    """.trimIndent()

    fun processingStaleBefore(
        now: ZonedDateTime,
        properties: TransactionalProperties
    ): ZonedDateTime = now.minus(properties.polling.processingStaleTimeout)
}
