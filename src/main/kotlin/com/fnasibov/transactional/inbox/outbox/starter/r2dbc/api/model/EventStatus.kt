package com.fnasibov.transactional.inbox.outbox.starter.r2dbc.api.model

enum class EventStatus {
    PENDING,
    PROCESSING,
    PROCESSED,
    FAILED,
    DEAD_LETTER
}