package com.fnasibov.transactional.inbox.outbox.core.configuration

import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.context.properties.EnableConfigurationProperties

/**
 * Base auto-configuration for the Transactional Inbox/Outbox starter.
 *
 * The starter is activated only when `transactional.enabled=true`.
 */
@AutoConfiguration
@ConditionalOnProperty(
    "transactional.enabled",
    havingValue = "true",
    matchIfMissing = false
)
@EnableConfigurationProperties(TransactionalProperties::class)
class TransactionalInboxOutboxAutoconfiguration
