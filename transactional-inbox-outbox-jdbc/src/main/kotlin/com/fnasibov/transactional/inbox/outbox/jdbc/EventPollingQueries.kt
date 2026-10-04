package com.fnasibov.transactional.inbox.outbox.jdbc

import com.fnasibov.transactional.inbox.outbox.core.configuration.TransactionalProperties
import java.time.ZonedDateTime

internal object EventPollingQueries {
    fun selectIdsSql(tableName: String): String =
        """
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

    fun updateStatusSql(tableName: String): String =
        """
        UPDATE $tableName
        SET status = :processingStatus,
            last_attempt_at = :now,
            updated_at = :now,
            next_retry_at = NULL
        WHERE id IN (:ids)
        """.trimIndent()

    fun processingStaleBefore(
        now: ZonedDateTime,
        properties: TransactionalProperties,
    ): ZonedDateTime = now.minus(properties.polling.processingStaleTimeout)

    /**
     * Selects the batch key of the next fetchable key group.
     *
     * Returns the key value of the oldest event that is eligible for processing
     * and whose key has no active claim. An active claim is a row in
     * `PROCESSING` whose last attempt is newer than the processing stale
     * timeout, i.e. a batch that another instance is still working on.
     */
    fun selectBatchKeySql(
        tableName: String,
        keyColumn: String,
    ): String =
        """
        SELECT src.$keyColumn
        FROM $tableName src
        WHERE (
            status = :pendingStatus
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
        )
        AND NOT EXISTS (
            SELECT 1
            FROM $tableName claimed
            WHERE (claimed.$keyColumn = src.$keyColumn
                OR (claimed.$keyColumn IS NULL AND src.$keyColumn IS NULL))
            AND claimed.status = :processingStatus
            AND claimed.last_attempt_at IS NOT NULL
            AND claimed.last_attempt_at >= :processingStaleBefore
        )
        ORDER BY src.created_at ASC
        LIMIT 1
        """.trimIndent()

    /**
     * Locks every non-terminal row of one batch key group with `FOR UPDATE`.
     *
     * The lock is held only for the duration of the fetch transaction and
     * serializes concurrent fetch attempts for the same key so that exactly one
     * transaction can observe the group before a claim is committed. Terminal
     * rows (`PROCESSED`, `DEAD_LETTER`) cannot change anymore and are excluded.
     */
    fun lockBatchKeyGroupSql(
        tableName: String,
        keyColumn: String,
        keyIsNull: Boolean,
    ): String =
        """
        SELECT id
        FROM $tableName
        WHERE ${keyMatch(keyColumn, keyIsNull)}
        AND status <> :processedStatus
        AND status <> :deadLetterStatus
        FOR UPDATE
        """.trimIndent()

    /**
     * Selects the event ids of one batch key group to claim.
     *
     * Mirrors the default eligibility rules but restricts the result to rows
     * matching the batch key and refuses to return anything while the key holds
     * an active claim (see [selectBatchKeySql]).
     */
    fun selectIdsForBatchKeySql(
        tableName: String,
        keyColumn: String,
        keyIsNull: Boolean,
    ): String =
        """
        SELECT id
        FROM $tableName
        WHERE ${keyMatch(keyColumn, keyIsNull)}
        AND (
            status = :pendingStatus
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
        )
        AND NOT EXISTS (
            SELECT 1
            FROM $tableName claimed
            WHERE ${keyMatch(keyColumn, keyIsNull, "claimed.")}
            AND claimed.status = :processingStatus
            AND claimed.last_attempt_at IS NOT NULL
            AND claimed.last_attempt_at >= :processingStaleBefore
        )
        ORDER BY created_at ASC
        LIMIT :limit
        """.trimIndent()

    private fun keyMatch(
        keyColumn: String,
        keyIsNull: Boolean,
        prefix: String = "",
    ): String =
        if (keyIsNull) {
            "${prefix}$keyColumn IS NULL"
        } else {
            "${prefix}$keyColumn = :batchKey"
        }
}
