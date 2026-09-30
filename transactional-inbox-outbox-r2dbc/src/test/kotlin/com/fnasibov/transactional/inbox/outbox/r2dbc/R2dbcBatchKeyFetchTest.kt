package com.fnasibov.transactional.inbox.outbox.r2dbc

import com.fnasibov.transactional.inbox.outbox.core.api.model.BaseEvent
import com.fnasibov.transactional.inbox.outbox.core.api.model.BatchKey
import com.fnasibov.transactional.inbox.outbox.core.api.model.EventStatus
import com.fnasibov.transactional.inbox.outbox.core.configuration.TransactionalProperties
import io.mockk.every
import io.mockk.mockk
import io.r2dbc.spi.Parameter
import io.r2dbc.spi.Row
import io.r2dbc.spi.RowMetadata
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.data.relational.core.mapping.Table
import org.springframework.data.relational.core.query.Query
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.r2dbc.core.FetchSpec
import org.springframework.r2dbc.core.RowsFetchSpec
import org.springframework.transaction.ReactiveTransaction
import org.springframework.transaction.reactive.TransactionCallback
import org.springframework.transaction.reactive.TransactionalOperator
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.ZonedDateTime
import java.util.UUID
import java.util.function.BiFunction
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
class R2dbcBatchKeyFetchTest {

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

    /**
     * Nullable-typed values are bound through the reified `bind` extension of
     * the reactive client and arrive wrapped in an r2dbc [Parameter]; the fake
     * stores the plain value.
     */
    private fun unwrapParameter(value: Any?): Any? =
        if (value is Parameter) value.value else value

    private fun batchSize(size: Int): TransactionalProperties = TransactionalProperties(
        polling = TransactionalProperties.Polling(batchSize = size)
    )

    private fun repository(
        store: FakeEventStore,
        properties: TransactionalProperties
    ): R2dbcEventRepository {
        var currentSql = ""
        var bindings: MutableMap<String, Any?> = mutableMapOf()

        val rowsFetch = mockk<RowsFetchSpec<Any>>()
        every { rowsFetch.all() } answers {
            Flux.fromIterable(store.query(currentSql, bindings))
        }

        val rowsUpdatedFetch = mockk<FetchSpec<Map<String, Any>>>()
        every { rowsUpdatedFetch.rowsUpdated() } answers {
            Mono.just(store.update(currentSql, bindings).toLong())
        }

        val executeSpec = mockk<DatabaseClient.GenericExecuteSpec>(relaxed = true)
        every { executeSpec.bind(any<String>(), any()) } answers {
            bindings[firstArg<String>()] = unwrapParameter(secondArg<Any?>())
            executeSpec
        }
        every {
            executeSpec.map(any<BiFunction<Row, RowMetadata, Any>>())
        } returns rowsFetch
        every { executeSpec.fetch() } returns rowsUpdatedFetch

        val databaseClient = mockk<DatabaseClient>()
        every { databaseClient.sql(any<String>()) } answers {
            currentSql = firstArg<String>()
            bindings = mutableMapOf()
            executeSpec
        }

        val template = mockk<R2dbcEntityTemplate>()
        every { template.databaseClient } returns databaseClient
        every {
            template.select(any<Query>(), any<Class<*>>())
        } answers {
            Flux.fromIterable(store.findAll(store.lastSelectedIds))
        }

        val transactionalOperator = mockk<TransactionalOperator>()
        every { transactionalOperator.execute(any<TransactionCallback<*>>()) } answers {
            val callback = firstArg<TransactionCallback<List<BaseEvent>>>()
            Flux.from(callback.doInTransaction(mockk<ReactiveTransaction>(relaxed = true)))
        }

        return R2dbcEventRepository(
            template = template,
            properties = properties,
            transactionalOperator = transactionalOperator,
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
 * [R2dbcEventRepository] with the semantics the statements are documented to
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

    fun findAll(ids: List<UUID>): List<BaseEvent> = ids.map { id ->
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

    fun query(sql: String, bindings: Map<String, Any?>): List<Any> {
        executedSql += sql
        return when {
            sql.contains("SKIP LOCKED") -> legacySelectIds(bindings)
            sql.contains("LIMIT 1") -> pickBatchKey(bindings)
            sql.contains("FOR UPDATE") -> lockBatchKeyGroup(bindings)
            else -> selectIdsForBatchKey(bindings)
        }
    }

    fun update(sql: String, bindings: Map<String, Any?>): Int {
        executedSql += sql
        return when {
            sql.contains("WHERE id IN (:ids)") -> claim(bindings)
            sql.contains("retry_count = :retryCount") -> markFailed(bindings)
            else -> markCompleted(bindings)
        }
    }

    private fun pickBatchKey(bindings: Map<String, Any?>): List<Any> {
        val now = bindings.time("now")
        val staleBefore = bindings.time("processingStaleBefore")
        val candidate = rows
            .filter { eligible(it, now, staleBefore) }
            .filter { candidate -> rows.none { sameKey(it, candidate) && claimed(it, staleBefore) } }
            .minByOrNull { it.createdAt }
        return if (candidate == null) emptyList() else listOf(candidate.key ?: nullBatchKeyMarker)
    }

    private fun lockBatchKeyGroup(bindings: Map<String, Any?>): List<Any> =
        rows
            .filter { matchesKey(it, bindings) && !terminal(it) }
            .mapNotNull { it.event.id }

    private fun selectIdsForBatchKey(bindings: Map<String, Any?>): List<Any> {
        val now = bindings.time("now")
        val staleBefore = bindings.time("processingStaleBefore")
        if (rows.any { matchesKey(it, bindings) && claimed(it, staleBefore) }) {
            return emptyList()
        }
        val ids = rows
            .filter { matchesKey(it, bindings) && eligible(it, now, staleBefore) }
            .sortedBy { it.createdAt }
            .take(bindings.limit())
            .mapNotNull { it.event.id }
        lastSelectedIds = ids
        return ids
    }

    private fun legacySelectIds(bindings: Map<String, Any?>): List<Any> {
        val now = bindings.time("now")
        val staleBefore = bindings.time("processingStaleBefore")
        val ids = rows
            .filter { eligible(it, now, staleBefore) }
            .sortedBy { it.createdAt }
            .take(bindings.limit())
            .mapNotNull { it.event.id }
        lastSelectedIds = ids
        return ids
    }

    private fun claim(bindings: Map<String, Any?>): Int {
        val now = bindings.time("now") ?: ZonedDateTime.now()
        val ids = bindings["ids"] as List<*>
        rows.filter { it.event.id in ids }.forEach {
            it.status = EventStatus.PROCESSING
            it.lastAttemptAt = now
            it.nextRetryAt = null
        }
        lastClaimedIds = ids
        return ids.size
    }

    private fun markCompleted(bindings: Map<String, Any?>): Int {
        val id = bindings["id"] as UUID
        val row = rows.single { it.event.id == id }
        row.status = EventStatus.valueOf(bindings["status"] as String)
        row.nextRetryAt = null
        return 1
    }

    private fun markFailed(bindings: Map<String, Any?>): Int {
        val id = bindings["id"] as UUID
        val row = rows.single { it.event.id == id }
        row.status = EventStatus.valueOf(bindings["status"] as String)
        row.lastAttemptAt = bindings.time("now")
        row.nextRetryAt = if (bindings.containsKey("nextRetryAt")) bindings.time("nextRetryAt") else null
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

    private fun matchesKey(row: Row, bindings: Map<String, Any?>): Boolean =
        if (bindings.containsKey("batchKey")) row.key == bindings["batchKey"]
        else row.key == null

    private fun Map<String, Any?>.limit(): Int = (getValue("limit") as Number).toInt()

    private fun Map<String, Any?>.time(name: String): ZonedDateTime? =
        when (val value = this[name]) {
            null -> null
            is ZonedDateTime -> value
            else -> error("Unsupported time value for $name: $value")
        }
}
