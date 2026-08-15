package com.fnasibov.transactional.inbox.outbox.r2dbc

import com.fnasibov.transactional.inbox.outbox.core.api.model.BaseEvent
import org.junit.jupiter.api.Test
import org.springframework.data.r2dbc.mapping.R2dbcMappingContext
import org.springframework.data.relational.core.mapping.Table
import kotlin.test.assertEquals

class R2dbcKotlinMappingContextTest {

    @Test
    fun `mapping context initializes Kotlin event entity`() {
        val mappingContext = R2dbcMappingContext().apply {
            setInitialEntitySet(setOf(SmokeTestEvent::class.java))
            afterPropertiesSet()
        }

        val entity = mappingContext.getRequiredPersistentEntity(SmokeTestEvent::class.java)

        assertEquals("smoke_test_events", entity.tableName.reference)
    }

    @Table("smoke_test_events")
    private class SmokeTestEvent(
        val payload: String = "test"
    ) : BaseEvent()
}
