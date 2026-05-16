package com.fnasibov.transactional.inbox.outbox.starter.r2dbc.domain

import com.fnasibov.transactional.inbox.outbox.starter.r2dbc.api.model.Event
import com.fnasibov.transactional.inbox.outbox.starter.r2dbc.api.model.EventStatus
import com.fnasibov.transactional.inbox.outbox.starter.r2dbc.configuration.TransactionalProperties
import kotlinx.coroutines.reactive.awaitSingle
import kotlinx.coroutines.reactor.awaitSingle
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.data.relational.core.mapping.Table
import org.springframework.data.relational.core.query.Criteria.where
import org.springframework.data.relational.core.query.Query
import org.springframework.transaction.ReactiveTransactionManager
import org.springframework.transaction.reactive.TransactionalOperator
import reactor.core.publisher.Mono
import java.time.ZonedDateTime
import java.util.UUID
import kotlin.jvm.java

class BaseEventRepository(
    private val template: R2dbcEntityTemplate,
    reactiveTransactionManager: ReactiveTransactionManager,
    private val properties: TransactionalProperties
) : EventRepository {

    private val transactionalOperator = TransactionalOperator.create(reactiveTransactionManager)


    override suspend fun <E : Event> fetchBatch(eventType: Class<E>): List<E> {
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

    override suspend fun <E : Event> save(event: E): E {
        val eventClass = event.javaClass

        val exists = template.exists(
            Query.query(where("id").`is`(event.id)),
            eventClass
        ).awaitSingle()
        return if (exists) {
            template.update(event)
                .awaitSingle()
        } else {
            template.insert(event)
                .awaitSingle()
        }
    }

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

    override suspend fun <E : Event> markAsFailed(event: E) {
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
    }

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