package com.fnasibov.transactional.inbox.outbox.jdbc

import com.fnasibov.transactional.inbox.outbox.core.api.model.BaseEvent
import com.fnasibov.transactional.inbox.outbox.core.api.model.BatchKey
import com.fnasibov.transactional.inbox.outbox.core.api.model.EventStatus
import com.fnasibov.transactional.inbox.outbox.core.configuration.TransactionalProperties
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.springframework.data.jdbc.core.JdbcAggregateOperations
import org.springframework.data.relational.core.mapping.Table
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations
import org.springframework.jdbc.core.namedparam.SqlParameterSource
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.support.TransactionTemplate
import java.time.OffsetDateTime
import java.time.ZonedDateTime
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Regression tests for the user-visible `@BatchKey` fetch contract and the
 * key exclusion protocol, executed against a stateful fake of the polling SQL.
 *
 * The fake implements the semantics the batch-key statements are documented to
 * provide (eligibility, durable claims, guard against actively claimed keys),
 * so these tests pin the repository behavior across the whole claim lifecycle.
 */
class JdbcBatchKeyFetchTest {

    @Test
    fun `annotated fetch returns one key group capped at the batch size`() = runBlocking {
        val store = FakeEventStore()
        val first = store.add(BatchKeyEvent(accountId = "account-a", createdAt = baseTime), "account-a")
        val second = store.add(BatchKeyEvent(accountId = "account-a", createdAt = baseTime.plusSeconds(1)), "account-a")
        val third = store.add(BatchKeyEvent(accountId = "account-b", createdAt = baseTime.plusSeconds(2)), "account-b")
        val fourth = store.add(BatchKeyEvent(accountId = "account-a", createdAt = baseTime.plusSeconds(3)), "account-a")
        val repository = repository(store, batchSize(2))

        val batch = repository.fetchBatch(BatchKeyEvent::class.java)

        assertEquals(listOf(first.event, second.event), batch)
        assertEquals(1, batch.map { it.accountId }.distinct().size)
        assertEquals(EventStatus.PROCESSING, store.rowOf(first.event).status)
        assertEquals(EventStatus.PROCESSING, store.rowOf(second.event).status)
        assertEquals(EventStatus.PENDING, store.rowOf(third.event).status)
        assertEquals(EventStatus.PENDING, store.rowOf(fourth.event).status)
    }

    @Test
    fun `a claimed key cannot be claimed again while its batch is processing`() = runBlocking {
        val store = FakeEventStore()
        store.add(BatchKeyEvent(accountId = "account-a", createdAt = baseTime), "account-a")
        store.add(BatchKeyEvent(accountId = "account-a", createdAt = baseTime.plusSeconds(1)), "account-a")
        store.add(BatchKeyEvent(accountId = "account-a", createdAt = baseTime.plusSeconds(2)), "account-a")
        val repository = repository(store, batchSize(2))

        val first = repository.fetchBatch(BatchKeyEvent::class.java)
        val second = repository.fetchBatch(BatchKeyEvent::class.java)

        assertEquals(2, first.size)
        assertEquals(
            emptyList(),
            second,
            "the third event must stay unclaimed while the first batch is processing"
        )
    }

    @Test
    fun `different keys stay claimable while another key is being processed`() = runBlocking {
        val store = FakeEventStore()
        val a1 = store.add(BatchKeyEvent(accountId = "account-a", createdAt = baseTime), "account-a")
        val a2 = store.add(BatchKeyEvent(accountId = "account-a", createdAt = baseTime.plusSeconds(1)), "account-a")
        val b1 = store.add(BatchKeyEvent(accountId = "account-b", createdAt = baseTime.plusSeconds(2)), "account-b")
        val repository = repository(store, batchSize(1))

        assertEquals(listOf(a1.event), repository.fetchBatch(BatchKeyEvent::class.java))
        assertEquals(
            listOf(b1.event),
            repository.fetchBatch(BatchKeyEvent::class.java),
            "a different key must remain claimable while account-a is processing"
        )
        assertEquals(emptyList(), repository.fetchBatch(BatchKeyEvent::class.java))

        repository.markAsProcessed(a1.event)
        repository.markAsProcessed(b1.event)

        assertEquals(listOf(a2.event), repository.fetchBatch(BatchKeyEvent::class.java))
    }

    @Test
    fun `key claim is released only when the last event of the batch completes`() = runBlocking {
        val store = FakeEventStore()
        val a1 = store.add(BatchKeyEvent(accountId = "account-a", createdAt = baseTime), "account-a")
        val a2 = store.add(BatchKeyEvent(accountId = "account-a", createdAt = baseTime.plusSeconds(1)), "account-a")
        val a3 = store.add(BatchKeyEvent(accountId = "account-a", createdAt = baseTime.plusSeconds(2)), "account-a")
        val repository = repository(store, batchSize(2))

        assertEquals(listOf(a1.event, a2.event), repository.fetchBatch(BatchKeyEvent::class.java))

        repository.markAsProcessed(a1.event)
        assertEquals(
            emptyList(),
            repository.fetchBatch(BatchKeyEvent::class.java),
            "the key stays claimed until the whole batch finished processing"
        )

        repository.markAsProcessed(a2.event)
        assertEquals(listOf(a3.event), repository.fetchBatch(BatchKeyEvent::class.java))
    }

    @Test
    fun `key claim recovers after the processing stale timeout`() = runBlocking {
        val store = FakeEventStore()
        val a1 = store.add(BatchKeyEvent(accountId = "account-a", createdAt = baseTime), "account-a")
        val a2 = store.add(BatchKeyEvent(accountId = "account-a", createdAt = baseTime.plusSeconds(1)), "account-a")
        val repository = repository(store, batchSize(2))

        assertEquals(listOf(a1.event, a2.event), repository.fetchBatch(BatchKeyEvent::class.java))
        assertEquals(emptyList(), repository.fetchBatch(BatchKeyEvent::class.java))

        store.expireClaims()

        assertEquals(
            listOf(a1.event, a2.event),
            repository.fetchBatch(BatchKeyEvent::class.java),
            "an abandoned claim must expire with the stale-processing lease"
        )
    }

    @Test
    fun `annotated fetch issues the key exclusion protocol`() = runBlocking {
        val store = FakeEventStore()
        val first = store.add(BatchKeyEvent(accountId = "account-a", createdAt = baseTime), "account-a")
        store.add(BatchKeyEvent(accountId = "account-b", createdAt = baseTime.plusSeconds(1)), "account-b")
        val repository = repository(store, batchSize(15))

        val batch = repository.fetchBatch(BatchKeyEvent::class.java)

        val pickSql = store.executedSql.single { it.contains("LIMIT 1") }
        assertTrue(pickSql.contains("NOT EXISTS"), "key pick must exclude actively claimed keys")
        assertTrue(pickSql.contains("last_attempt_at >= :processingStaleBefore"))

        val lockSql = store.executedSql.single { it.contains("FOR UPDATE") }
        assertTrue(lockSql.contains("account_id = :batchKey"), "group lock must target the key column")
        assertTrue(!lockSql.contains("SKIP LOCKED"), "group lock must serialize same-key fetchers")

        val selectSql = store.executedSql.single { it.contains("LIMIT :limit") }
        assertTrue(selectSql.contains("NOT EXISTS"), "claim select must guard against active claims")
        assertTrue(selectSql.contains("account_id = :batchKey"))

        assertEquals(listOf(first.event), batch)
        assertEquals(listOf(first.event.id), store.lastClaimedIds, "only returned events may be claimed")
    }

    @Test
    fun `unannotated models keep the legacy skip locked fetch`() = runBlocking {
        val store = FakeEventStore()
        val first = store.add(LegacyEvent(accountId = "account-a", createdAt = baseTime), "account-a")
        val second = store.add(LegacyEvent(accountId = "account-b", createdAt = baseTime.plusSeconds(1)), "account-b")
        val repository = repository(store, batchSize(15))

        val batch = repository.fetchBatch(LegacyEvent::class.java)

        assertEquals(listOf(first.event, second.event), batch)
        assertTrue(store.executedSql.any { it.contains("FOR UPDATE SKIP LOCKED") })
        assertTrue(
            store.executedSql.none { it.contains("NOT EXISTS") },
            "the legacy fetch path must stay free of the key exclusion protocol"
        )
    }

    private val baseTime: ZonedDateTime = ZonedDateTime.parse("2026-01-01T00:00:00Z")

    private fun batchSize(size: Int): TransactionalProperties = TransactionalProperties(
        polling = TransactionalProperties.Polling(batchSize = size)
    )

    private fun repository(
        store: FakeEventStore,
        properties: TransactionalProperties
    ): JdbcEventRepository {
        val jdbc = mockk<NamedParameterJdbcOperations>(relaxed = true)
        every {
            jdbc.queryForList(any<String>(), any<SqlParameterSource>(), any<Class<*>>())
        } answers {
            store.query(firstArg<String>(), secondArg<SqlParameterSource>())
        }
        every {
            jdbc.update(any<String>(), any<SqlParameterSource>())
        } answers {
            store.update(firstArg<String>(), secondArg<SqlParameterSource>())
        }

        val aggregates = mockk<JdbcAggregateOperations>(relaxed = true)
        every {
            aggregates.findAllById(any(), any<Class<*>>())
        } answers {
            store.findAll(firstArg<Iterable<*>>())
        }

        return JdbcEventRepository(
            jdbc = jdbc,
            aggregates = aggregates,
            transactionTemplate = TransactionTemplate(
                mockk<PlatformTransactionManager>(relaxed = true)
            ),
            properties = properties,
            strategiesByEventType = emptyMap()
        )
    }

    @Table("batch_key_events")
    private class BatchKeyEvent(
        @BatchKey val accountId: String,
        id: UUID = UUID.randomUUID(),
        createdAt: ZonedDateTime = ZonedDateTime.now()
    ) : BaseEvent(id = id, createdAt = createdAt)

    @Table("legacy_events")
    private class LegacyEvent(
        val accountId: String,
        id: UUID = UUID.randomUUID(),
        createdAt: ZonedDateTime = ZonedDateTime.now()
    ) : BaseEvent(id = id, createdAt = createdAt)
}

/**
 * In-memory stand-in for the event table that interprets the polling SQL of
 * [JdbcEventRepository] with the semantics the statements are documented to
 * provide: eligibility rules, durable `PROCESSING` claims per batch key, and
 * the guard that refuses to select rows of actively claimed keys.
 */
internal class FakeEventStore {

    data class Row(
        val event: BaseEvent,
        val key: Any?,
        val createdAt: ZonedDateTime,
        var status: EventStatus,
        var lastAttemptAt: ZonedDateTime?,
        var nextRetryAt: ZonedDateTime?
    )

    val rows = mutableListOf<Row>()
    val executedSql = mutableListOf<String>()
    var lastSelectedIds: List<UUID> = emptyList()
    var lastClaimedIds: List<Any?> = emptyList()

    fun add(event: BaseEvent, key: Any?): Row =
        Row(event, key, event.createdAt, event.status, event.lastAttemptAt, event.nextRetryAt)
            .also { rows += it }

    fun rowOf(event: BaseEvent): Row = rows.single { it.event === event }

    fun findAll(ids: Iterable<*>): List<BaseEvent> = ids.map { id ->
        rows.single { it.event.id == id }.event
    }

    /** Simulates an instance crash: every active claim ages past the lease. */
    fun expireClaims() {
        rows.forEach { row ->
            if (row.status == EventStatus.PROCESSING) {
                row.lastAttemptAt = ZonedDateTime.now().minusHours(1)
            }
        }
    }

    fun query(sql: String, params: SqlParameterSource): List<Any?> {
        executedSql += sql
        return when {
            sql.contains("SKIP LOCKED") -> legacySelectIds(params)
            sql.contains("LIMIT 1") -> pickBatchKey(params)
            sql.contains("FOR UPDATE") -> lockBatchKeyGroup(params)
            else -> selectIdsForBatchKey(params)
        }
    }

    fun update(sql: String, params: SqlParameterSource): Int {
        executedSql += sql
        return when {
            sql.contains("WHERE id IN (:ids)") -> claim(params)
            sql.contains("retry_count = :retryCount") -> markFailed(params)
            else -> markCompleted(params)
        }
    }

    private fun pickBatchKey(params: SqlParameterSource): List<Any?> {
        val now = params.time("now")
        val staleBefore = params.time("processingStaleBefore")
        val candidate = rows
            .filter { eligible(it, now, staleBefore) }
            .filter { candidate -> rows.none { sameKey(it, candidate) && claimed(it, staleBefore) } }
            .minByOrNull { it.createdAt }
        return if (candidate == null) emptyList() else listOf(candidate.key)
    }

    private fun lockBatchKeyGroup(params: SqlParameterSource): List<Any?> =
        rows
            .filter { matchesKey(it, params) && !terminal(it) }
            .map { it.event.id }

    private fun selectIdsForBatchKey(params: SqlParameterSource): List<Any?> {
        val now = params.time("now")
        val staleBefore = params.time("processingStaleBefore")
        if (rows.any { matchesKey(it, params) && claimed(it, staleBefore) }) {
            return emptyList()
        }
        val ids = rows
            .filter { matchesKey(it, params) && eligible(it, now, staleBefore) }
            .sortedBy { it.createdAt }
            .take(params.limit())
            .mapNotNull { it.event.id }
        lastSelectedIds = ids
        return ids
    }

    private fun legacySelectIds(params: SqlParameterSource): List<Any?> {
        val now = params.time("now")
        val staleBefore = params.time("processingStaleBefore")
        val ids = rows
            .filter { eligible(it, now, staleBefore) }
            .sortedBy { it.createdAt }
            .take(params.limit())
            .mapNotNull { it.event.id }
        lastSelectedIds = ids
        return ids
    }

    private fun claim(params: SqlParameterSource): Int {
        val now = params.time("now") ?: ZonedDateTime.now()
        val ids = params.getValue("ids") as List<*>
        rows.filter { it.event.id in ids }.forEach {
            it.status = EventStatus.PROCESSING
            it.lastAttemptAt = now
            it.nextRetryAt = null
        }
        lastClaimedIds = ids
        return ids.size
    }

    private fun markCompleted(params: SqlParameterSource): Int {
        val id = params.getValue("id") as UUID
        val row = rows.single { it.event.id == id }
        row.status = EventStatus.valueOf(params.getValue("status") as String)
        row.nextRetryAt = null
        return 1
    }

    private fun markFailed(params: SqlParameterSource): Int {
        val id = params.getValue("id") as UUID
        val row = rows.single { it.event.id == id }
        row.status = EventStatus.valueOf(params.getValue("status") as String)
        row.lastAttemptAt = params.time("now")
        row.nextRetryAt = if (params.hasValue("nextRetryAt")) params.time("nextRetryAt") else null
        return 1
    }

    private fun eligible(row: Row, now: ZonedDateTime?, staleBefore: ZonedDateTime?): Boolean =
        row.status == EventStatus.PENDING ||
            (row.status == EventStatus.PROCESSING &&
                row.lastAttemptAt != null &&
                row.lastAttemptAt!! < staleBefore!!) ||
            (row.status == EventStatus.FAILED &&
                (row.nextRetryAt == null || !row.nextRetryAt!!.isAfter(now!!)))

    private fun claimed(row: Row, staleBefore: ZonedDateTime?): Boolean =
        row.status == EventStatus.PROCESSING &&
            row.lastAttemptAt != null &&
            !row.lastAttemptAt!!.isBefore(staleBefore!!)

    private fun terminal(row: Row): Boolean =
        row.status == EventStatus.PROCESSED || row.status == EventStatus.DEAD_LETTER

    private fun sameKey(first: Row, second: Row): Boolean = first.key == second.key

    private fun matchesKey(row: Row, params: SqlParameterSource): Boolean =
        if (params.hasValue("batchKey")) row.key == params.getValue("batchKey")
        else row.key == null

    private fun SqlParameterSource.limit(): Int = (getValue("limit") as Number).toInt()

    private fun SqlParameterSource.time(name: String): ZonedDateTime? =
        when (val value = getValue(name)) {
            null -> null
            is ZonedDateTime -> value
            is OffsetDateTime -> value.toZonedDateTime()
            else -> error("Unsupported time value for $name: $value")
        }
}
