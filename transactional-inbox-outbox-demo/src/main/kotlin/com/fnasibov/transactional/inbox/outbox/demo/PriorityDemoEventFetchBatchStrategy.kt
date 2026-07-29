package com.fnasibov.transactional.inbox.outbox.demo

import com.fnasibov.transactional.inbox.outbox.core.api.FetchBatchStrategy
import com.fnasibov.transactional.inbox.outbox.core.api.model.EventStatus
import com.fnasibov.transactional.inbox.outbox.core.configuration.TransactionalProperties
import kotlinx.coroutines.reactive.awaitSingle
import kotlinx.coroutines.reactor.awaitSingle
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.data.relational.core.query.Criteria.where
import org.springframework.data.relational.core.query.Query
import org.springframework.stereotype.Component
import org.springframework.transaction.reactive.TransactionalOperator
import reactor.core.publisher.Mono
import java.time.ZonedDateTime
import java.util.UUID

@Component
class PriorityDemoEventFetchBatchStrategy(
    private val template: R2dbcEntityTemplate,
    private val transactionalOperator: TransactionalOperator,
    private val properties: TransactionalProperties
) : FetchBatchStrategy<DemoEvent> {

    override val eventType: Class<DemoEvent> =
        DemoEvent::class.java

    override suspend fun fetchBatch(): List<DemoEvent> =
        transactionalOperator.execute {
            val now = ZonedDateTime.now()

            template.databaseClient.sql(SELECT_IDS_SQL)
                .bind("status", EventStatus.PENDING.name)
                .bind("limit", properties.polling.batchSize)
                .map { row, _ -> row.get("id", UUID::class.java)!! }
                .all()
                .collectList()
                .flatMap { ids ->
                    if (ids.isEmpty()) {
                        return@flatMap Mono.just(emptyList())
                    }

                    template.databaseClient.sql(UPDATE_STATUS_SQL)
                        .bind("status", EventStatus.PROCESSING.name)
                        .bind("updatedAt", now)
                        .bind("ids", ids)
                        .fetch()
                        .rowsUpdated()
                        .thenMany(
                            template.select(
                                Query.query(where("id").`in`(ids)),
                                DemoEvent::class.java
                            )
                        )
                        .collectList()
                }
        }.awaitSingle()

    private companion object {
        val SELECT_IDS_SQL = """
            SELECT id
            FROM demo_events
            WHERE status = :status
            ORDER BY priority DESC, created_at ASC
            FOR UPDATE SKIP LOCKED
            LIMIT :limit
        """.trimIndent()

        val UPDATE_STATUS_SQL = """
            UPDATE demo_events
            SET status = :status,
                updated_at = :updatedAt
            WHERE id IN (:ids)
        """.trimIndent()
    }
}
