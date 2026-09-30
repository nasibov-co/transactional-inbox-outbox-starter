package com.fnasibov.transactional.inbox.outbox.core.api.model

import org.junit.jupiter.api.Test
import kotlin.reflect.full.findAnnotation
import kotlin.reflect.full.memberProperties
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

class BatchKeyTest {

    @Test
    fun `batch key is discoverable on exactly one model property`() {
        val annotated = BatchKeyModel::class.memberProperties
            .filter { it.findAnnotation<BatchKey>() != null }

        assertEquals(1, annotated.size)
        assertEquals("accountId", annotated.single().name)
    }

    @Test
    fun `batch key has runtime retention`() {
        val retention = BatchKey::class.annotations
            .filterIsInstance<Retention>()
            .single()

        assertEquals(AnnotationRetention.RUNTIME, retention.value)
    }

    @Test
    fun `column name defaults to an empty value`() {
        val annotation = BatchKeyModel::class.memberProperties
            .mapNotNull { it.findAnnotation<BatchKey>() }
            .single()

        assertEquals("", annotation.columnName)
    }

    @Test
    fun `column name can be declared explicitly`() {
        val annotation = CustomColumnModel::class.memberProperties
            .mapNotNull { it.findAnnotation<BatchKey>() }
            .single()

        assertEquals("tenant_key", annotation.columnName)
    }
}

private data class BatchKeyModel(
    @BatchKey val accountId: String,
    val amount: Long
)

private data class CustomColumnModel(
    @BatchKey(columnName = "tenant_key") val accountId: String,
    val amount: Long
)
