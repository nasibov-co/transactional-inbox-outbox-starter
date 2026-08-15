package com.fnasibov.transactional.inbox.outbox.autoconfigure

import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.context.annotation.Bean
import org.springframework.core.convert.converter.Converter
import org.springframework.data.convert.ReadingConverter
import org.springframework.data.convert.WritingConverter
import org.springframework.data.jdbc.core.convert.JdbcCustomConversions
import org.springframework.data.jdbc.core.dialect.JdbcDialect
import java.sql.Timestamp
import java.time.OffsetDateTime
import java.time.ZoneId
import java.time.ZonedDateTime

@AutoConfiguration(
    beforeName = [
        "org.springframework.boot.data.jdbc.autoconfigure.DataJdbcRepositoriesAutoConfiguration"
    ]
)
@ConditionalOnClass(JdbcCustomConversions::class)
class TransactionalInboxOutboxJdbcConversionsAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(JdbcCustomConversions::class)
    fun transactionalInboxOutboxJdbcCustomConversions(
        dialect: JdbcDialect
    ): JdbcCustomConversions = JdbcCustomConversions.of(
        dialect,
        listOf(
            TimestampToZonedDateTimeConverter,
            OffsetDateTimeToZonedDateTimeConverter,
            ZonedDateTimeToOffsetDateTimeConverter
        )
    )
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
