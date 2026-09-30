package com.fnasibov.transactional.inbox.outbox.core.domain.exception

/**
 * Thrown when a batch handler returns outcome decisions that do not match the
 * processed batch, for example when an event id is omitted or an unknown id is
 * reported. The whole batch is retried in this case.
 */
class BatchResultValidationException(message: String) : RuntimeException(message) {
}
