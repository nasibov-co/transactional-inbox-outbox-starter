package com.fnasibov.transactional.inbox.outbox.demo.jdbc

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Lazy
import org.springframework.core.convert.converter.Converter
import org.springframework.data.convert.ReadingConverter
import org.springframework.data.convert.WritingConverter
import org.springframework.data.jdbc.core.convert.JdbcConverter
import org.springframework.data.jdbc.core.convert.JdbcCustomConversions
import org.springframework.data.jdbc.core.convert.RelationResolver
import org.springframework.data.jdbc.core.dialect.JdbcDialect
import org.springframework.data.jdbc.core.mapping.JdbcMappingContext
import org.springframework.data.jdbc.repository.config.JdbcConfiguration
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations
import java.sql.JDBCType
import java.sql.SQLType
import java.sql.Timestamp
import java.time.OffsetDateTime
import java.time.ZoneId
import java.time.ZonedDateTime

/**
 * Teaches Spring Data JDBC how to persist the demo's `ZonedDateTime` columns.
 *
 * Spring Data JDBC resolves `ZonedDateTime` properties to `String` columns and binds
 * them with `Types.VARCHAR`, while the demo tables use `TIMESTAMP WITH TIME ZONE` (the
 * processing engine reads and writes them as `OffsetDateTime`). PostgreSQL rejects the
 * resulting varchar expressions, so this configuration:
 *
 * - registers conversions between `ZonedDateTime` and the driver's `OffsetDateTime` /
 *   `Timestamp` values, and
 * - overrides the declared column type of `ZonedDateTime` properties to
 *   `TIMESTAMP WITH TIME ZONE`, which also fixes the binding of `NULL` values.
 *
 * The beans replace the starter's defaults (`@ConditionalOnMissingBean` there backs off)
 * and apply to every Spring Data JDBC repository and `JdbcAggregateOperations` in the
 * demo, so `repository.save` works against the demo schema.
 */
@Configuration(proxyBeanMethods = false)
class JdbcDemoConfiguration {

    @Bean
    fun jdbcDemoCustomConversions(dialect: JdbcDialect): JdbcCustomConversions =
        JdbcCustomConversions.of(
            dialect,
            listOf(
                TimestampToZonedDateTimeConverter,
                OffsetDateTimeToZonedDateTimeConverter,
                ZonedDateTimeToOffsetDateTimeConverter,
            )
        )

    @Bean
    fun jdbcDemoJdbcConverter(
        mappingContext: JdbcMappingContext,
        operations: NamedParameterJdbcOperations,
        @Lazy relationResolver: RelationResolver,
        conversions: JdbcCustomConversions,
        dialect: JdbcDialect,
    ): JdbcConverter = ZonedDateTimeAwareJdbcConverter(
        JdbcConfiguration.createConverter(mappingContext, operations, relationResolver, conversions, dialect)
    )
}

/**
 * [JdbcConverter] that reports `ZonedDateTime` columns as `TIMESTAMP WITH TIME ZONE`.
 *
 * Without this, even `NULL` values are bound with `Types.VARCHAR`, which PostgreSQL
 * rejects for the demo's `TIMESTAMP WITH TIME ZONE` columns.
 */
private class ZonedDateTimeAwareJdbcConverter(
    private val delegate: JdbcConverter,
) : JdbcConverter by delegate {

    override fun getColumnType(property: RelationalPersistentProperty): Class<*> =
        if (property.actualType == ZonedDateTime::class.java) {
            OffsetDateTime::class.java
        } else {
            delegate.getColumnType(property)
        }

    override fun getTargetSqlType(property: RelationalPersistentProperty): SQLType =
        if (property.actualType == ZonedDateTime::class.java) {
            JDBCType.TIMESTAMP_WITH_TIMEZONE
        } else {
            delegate.getTargetSqlType(property)
        }
}

@ReadingConverter
private object TimestampToZonedDateTimeConverter : Converter<Timestamp, ZonedDateTime> {
    override fun convert(source: Timestamp): ZonedDateTime =
        source.toInstant().atZone(ZoneId.systemDefault())
}

@ReadingConverter
private object OffsetDateTimeToZonedDateTimeConverter : Converter<OffsetDateTime, ZonedDateTime> {
    override fun convert(source: OffsetDateTime): ZonedDateTime =
        source.toZonedDateTime()
}

@WritingConverter
private object ZonedDateTimeToOffsetDateTimeConverter : Converter<ZonedDateTime, OffsetDateTime> {
    override fun convert(source: ZonedDateTime): OffsetDateTime =
        source.toOffsetDateTime()
}
