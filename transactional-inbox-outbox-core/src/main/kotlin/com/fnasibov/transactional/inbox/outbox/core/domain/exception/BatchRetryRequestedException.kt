package com.fnasibov.transactional.inbox.outbox.core.domain.exception

/**
 * Reported to batch dead-letter handling for events whose retry was requested by the
 * batch handler (`RETRY` outcome) and whose retry limit was exhausted. It stands in
 * for the processing failure that made the handler request a retry.
 */
class BatchRetryRequestedException(message: String) : RuntimeException(message) {
}
