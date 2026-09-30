package com.fnasibov.transactional.inbox.outbox.core.domain

import com.fnasibov.transactional.inbox.outbox.core.api.model.BatchKey
import com.fnasibov.transactional.inbox.outbox.core.api.model.Event
import java.lang.reflect.Field
import java.util.concurrent.ConcurrentHashMap
import kotlin.reflect.KProperty1
import kotlin.reflect.full.findAnnotation
import kotlin.reflect.full.memberProperties
import kotlin.reflect.jvm.javaField
import kotlin.reflect.jvm.javaMethod

/**
 * Resolves and applies the [BatchKey] grouping key declared by an event model.
 *
 * Persistence adapters use this support class to implement key-grouped batch
 * fetching: every fetched batch is homogeneous with respect to the annotated
 * key and never exceeds the configured batch size.
 */
object BatchKeySupport {

    private val descriptors = ConcurrentHashMap<Class<*>, DescriptorHolder>()

    /**
     * Resolves the [BatchKey] descriptor for an event model, or `null` when the
     * model declares no batch key.
     *
     * The descriptor exposes the physical column name backing the annotated
     * member and a reader for the key value of an event instance. The column
     * name is taken from the annotation's explicit `columnName` when set, and
     * otherwise derived from the member name using snake case, matching the
     * default Spring Data physical naming (`accountId` maps to `account_id`).
     *
     * @param eventType event class to inspect
     * @throws IllegalStateException if the model declares more than one batch key
     */
    fun batchKeyDescriptor(eventType: Class<*>): BatchKeyDescriptor? =
        descriptors.computeIfAbsent(eventType) { DescriptorHolder(resolve(it)) }.descriptor

    /**
     * Reduces a loaded set of events to a single batch key group.
     *
     * Events are grouped by their batch key value and only the group holding
     * the oldest event is kept, so a batch always contains the events of
     * exactly one key. The result is additionally capped at [batchSize].
     *
     * @param events loaded events, typically the result of a fetch
     * @param batchSize maximum number of events to keep
     * @param batchKey descriptor of the annotated key member
     * @return homogeneous batch of at most [batchSize] events
     */
    fun <E : Event> selectKeyGroup(
        events: List<E>,
        batchSize: Int,
        batchKey: BatchKeyDescriptor
    ): List<E> {
        if (events.isEmpty() || batchSize <= 0) {
            return emptyList()
        }

        val groups = LinkedHashMap<Any?, MutableList<E>>()
        for (event in events) {
            groups.getOrPut(batchKey.value(event)) { mutableListOf() }.add(event)
        }

        var oldest: List<E>? = null
        for (group in groups.values) {
            val current = oldest
            if (current == null || group.first().createdAt.isBefore(current.first().createdAt)) {
                oldest = group
            }
        }

        return (oldest ?: emptyList()).take(batchSize)
    }

    private fun resolve(eventType: Class<*>): BatchKeyDescriptor? {
        val members = LinkedHashMap<String, BatchKeyDescriptor>()

        for (property in eventType.kotlin.memberProperties) {
            val annotation = property.findAnnotation<BatchKey>()
            if (annotation != null) {
                members[property.name] = BatchKeyDescriptor(
                    columnName = resolveColumnName(annotation.columnName, property.name),
                    reader = propertyReader(property)
                )
            }
        }

        for (field in hierarchyFields(eventType)) {
            val annotation = field.getAnnotation(BatchKey::class.java)
            if (annotation != null) {
                members.putIfAbsent(
                    field.name,
                    BatchKeyDescriptor(
                        columnName = resolveColumnName(annotation.columnName, field.name),
                        reader = fieldReader(field)
                    )
                )
            }
        }

        if (members.size > 1) {
            error(
                "Event ${eventType.name} must declare at most one @BatchKey member, " +
                    "found: ${members.keys.sorted().joinToString(", ")}"
            )
        }

        return members.values.firstOrNull()
    }

    private fun propertyReader(property: KProperty1<*, *>): (Any) -> Any? {
        val field = property.javaField
        if (field != null) {
            return fieldReader(field)
        }

        val getter = property.getter.javaMethod
        if (getter != null) {
            getter.isAccessible = true
            return { getter.invoke(it) }
        }

        @Suppress("UNCHECKED_CAST")
        val accessor = property as KProperty1<Any, Any?>
        return { accessor.get(it) }
    }

    private fun fieldReader(field: Field): (Any) -> Any? {
        field.isAccessible = true
        return { field.get(it) }
    }

    private fun hierarchyFields(eventType: Class<*>): List<Field> {
        val fields = mutableListOf<Field>()
        var current: Class<*>? = eventType
        while (current != null && current != Any::class.java) {
            fields += current.declaredFields
            current = current.superclass
        }
        return fields
    }

    private fun resolveColumnName(declaredColumnName: String, memberName: String): String =
        declaredColumnName.ifBlank { snakeCase(memberName) }

    private fun snakeCase(name: String): String =
        name.replace(Regex("(?<=[a-z0-9])(?=[A-Z])"), "_").lowercase()

    private class DescriptorHolder(val descriptor: BatchKeyDescriptor?)
}

/**
 * Describes the [BatchKey] member of an event model.
 *
 * @param columnName physical column name backing the annotated member
 * @param reader reads the key value from an event instance
 */
class BatchKeyDescriptor internal constructor(
    val columnName: String,
    private val reader: (Any) -> Any?
) {

    /**
     * Reads the batch key value from the given event.
     */
    fun value(event: Any): Any? = reader(event)
}
