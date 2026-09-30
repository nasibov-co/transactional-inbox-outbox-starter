package com.fnasibov.transactional.inbox.outbox.core.domain

import com.fnasibov.transactional.inbox.outbox.core.api.model.BaseEvent
import com.fnasibov.transactional.inbox.outbox.core.api.model.BatchKey
import org.junit.jupiter.api.Test
import java.time.ZonedDateTime
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull

class BatchKeySupportTest {

    @Test
    fun `resolves annotated property to a snake case column`() {
        val descriptor = BatchKeySupport.batchKeyDescriptor(PropertyKeyedEvent::class.java)

        assertEquals("account_id", requireNotNull(descriptor).columnName)
        assertEquals("account-a", descriptor.value(PropertyKeyedEvent(accountId = "account-a")))
    }

    @Test
    fun `resolves field annotated batch key`() {
        val descriptor = BatchKeySupport.batchKeyDescriptor(FieldKeyedEvent::class.java)

        assertEquals("account_id", requireNotNull(descriptor).columnName)
        assertEquals("account-a", descriptor.value(FieldKeyedEvent(accountId = "account-a")))
    }

    @Test
    fun `uses explicit column name from a property annotation`() {
        val descriptor = BatchKeySupport.batchKeyDescriptor(CustomColumnKeyedEvent::class.java)

        assertEquals("tenant_key", requireNotNull(descriptor).columnName)
        assertEquals("tenant-a", descriptor.value(CustomColumnKeyedEvent(accountId = "tenant-a")))
    }

    @Test
    fun `uses explicit column name from a field annotation`() {
        val descriptor = BatchKeySupport.batchKeyDescriptor(CustomColumnFieldKeyedEvent::class.java)

        assertEquals("tenant_key", requireNotNull(descriptor).columnName)
        assertEquals("tenant-a", descriptor.value(CustomColumnFieldKeyedEvent(accountId = "tenant-a")))
    }

    @Test
    fun `blank column name falls back to the snake case member name`() {
        val descriptor = BatchKeySupport.batchKeyDescriptor(BlankColumnKeyedEvent::class.java)

        assertEquals("account_id", requireNotNull(descriptor).columnName)
    }

    @Test
    fun `returns null for models without a batch key`() {
        assertNull(BatchKeySupport.batchKeyDescriptor(PlainEvent::class.java))
    }

    @Test
    fun `rejects models with more than one batch key`() {
        assertFailsWith<IllegalStateException> {
            BatchKeySupport.batchKeyDescriptor(DuplicateKeyEvent::class.java)
        }
    }

    @Test
    fun `selectKeyGroup keeps a single key capped at the batch size`() {
        val descriptor = requireNotNull(BatchKeySupport.batchKeyDescriptor(PropertyKeyedEvent::class.java))
        val first = PropertyKeyedEvent(accountId = "account-a", createdAt = baseTime)
        val second = PropertyKeyedEvent(accountId = "account-b", createdAt = baseTime.plusSeconds(1))
        val third = PropertyKeyedEvent(accountId = "account-a", createdAt = baseTime.plusSeconds(2))

        val batch = BatchKeySupport.selectKeyGroup(listOf(first, second, third), 2, descriptor)

        assertEquals(listOf(first, third), batch)
    }

    @Test
    fun `selectKeyGroup picks the key of the oldest event`() {
        val descriptor = requireNotNull(BatchKeySupport.batchKeyDescriptor(PropertyKeyedEvent::class.java))
        val older = PropertyKeyedEvent(accountId = "account-a", createdAt = baseTime.minusSeconds(5))
        val newer = PropertyKeyedEvent(accountId = "account-b", createdAt = baseTime)

        val batch = BatchKeySupport.selectKeyGroup(listOf(newer, older), 15, descriptor)

        assertEquals(listOf(older), batch)
    }

    @Test
    fun `selectKeyGroup groups null keys together`() {
        val descriptor = requireNotNull(BatchKeySupport.batchKeyDescriptor(NullableKeyEvent::class.java))
        val first = NullableKeyEvent(accountId = null, createdAt = baseTime)
        val second = NullableKeyEvent(accountId = null, createdAt = baseTime.plusSeconds(1))
        val other = NullableKeyEvent(accountId = "account-a", createdAt = baseTime.plusSeconds(2))

        val batch = BatchKeySupport.selectKeyGroup(listOf(first, second, other), 15, descriptor)

        assertEquals(listOf(first, second), batch)
    }

    @Test
    fun `selectKeyGroup returns empty list for empty input`() {
        val descriptor = requireNotNull(BatchKeySupport.batchKeyDescriptor(PropertyKeyedEvent::class.java))

        assertEquals(emptyList(), BatchKeySupport.selectKeyGroup(emptyList(), 15, descriptor))
    }

    private val baseTime: ZonedDateTime = ZonedDateTime.parse("2026-01-01T00:00:00Z")

    private class PropertyKeyedEvent(
        @BatchKey val accountId: String,
        createdAt: ZonedDateTime = ZonedDateTime.now()
    ) : BaseEvent(
        id = UUID.randomUUID(),
        createdAt = createdAt
    )

    private class FieldKeyedEvent(
        @field:BatchKey val accountId: String
    ) : BaseEvent()

    private class CustomColumnKeyedEvent(
        @BatchKey(columnName = "tenant_key") val accountId: String
    ) : BaseEvent()

    private class CustomColumnFieldKeyedEvent(
        @field:BatchKey(columnName = "tenant_key") val accountId: String
    ) : BaseEvent()

    private class BlankColumnKeyedEvent(
        @BatchKey(columnName = "   ") val accountId: String
    ) : BaseEvent()

    private class NullableKeyEvent(
        @BatchKey val accountId: String?,
        createdAt: ZonedDateTime = ZonedDateTime.now()
    ) : BaseEvent(
        id = UUID.randomUUID(),
        createdAt = createdAt
    )

    private class DuplicateKeyEvent(
        @BatchKey val accountId: String,
        @BatchKey val orderId: String
    ) : BaseEvent()

    private class PlainEvent(
        val accountId: String
    ) : BaseEvent()
}
