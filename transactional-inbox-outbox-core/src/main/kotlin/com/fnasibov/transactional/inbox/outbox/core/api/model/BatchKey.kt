package com.fnasibov.transactional.inbox.outbox.core.api.model

/**
 * Marks the single model field or property that identifies the batch grouping key
 * for an event type.
 *
 * The annotated value is the domain identifier that a batch fetcher uses to group
 * related events together, for example a tenant, account, or aggregate identifier:
 *
 * ```
 * @Table("payment_events")
 * data class PaymentEvent(
 *     @BatchKey
 *     val accountId: UUID,
 *     val amount: Long
 * ) : BaseEvent()
 * ```
 *
 * Constraints and behavior:
 * - Apply this annotation to at most one field or property per event model.
 * - Members annotated this way must be readable so the grouping key can be resolved
 *   later, typically through reflection. The backing column name is derived from the
 *   member name using snake case (`accountId` maps to `account_id`) unless an explicit
 *   [columnName] is provided.
 * - When present, batch fetching is grouped by this key: each fetch returns events for
 *   at most one key value and at most the configured batch size. While a fetched batch
 *   for a key is being processed, other fetches cannot claim events with the same key;
 *   events with different keys remain independently fetchable. The key claim is held
 *   as long as at least one event of the claimed batch is in `PROCESSING` status and is
 *   released when the whole batch finishes. If the claiming instance crashes, the claim
 *   expires together with the processing stale timeout
 *   (`transactional.polling.processing-stale-timeout`) and the events become fetchable
 *   again, mirroring the existing stale-processing recovery.
 * - Event models without this annotation keep the default fetch behavior, where one
 *   batch may contain events with different key values.
 *
 * If no field or property is annotated, the event type has no declared grouping key.
 *
 * @property columnName optional physical database column name backing the annotated
 *   member. When blank (the default), the column name is derived from the member name
 *   using snake case (`accountId` maps to `account_id`). Set it explicitly, for example
 *   `@BatchKey(columnName = "tenant_key")`, when the physical column does not follow the
 *   default snake case derivation.
 */
@Target(
    AnnotationTarget.FIELD,
    AnnotationTarget.PROPERTY,
    AnnotationTarget.VALUE_PARAMETER
)
@Retention(AnnotationRetention.RUNTIME)
@MustBeDocumented
annotation class BatchKey(
    val columnName: String = ""
)
