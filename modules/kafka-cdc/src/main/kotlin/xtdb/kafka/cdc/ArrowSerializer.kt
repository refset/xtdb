package xtdb.kafka.cdc

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.module.kotlin.KotlinModule
import xtdb.time.Interval
import xtdb.vector.extensions.*
import java.time.*
import java.time.format.DateTimeFormatter
import java.util.UUID
import java.math.BigDecimal
import java.net.URI

/**
 * Custom serializer that properly handles all XTDB Arrow types for CDC output.
 * Converts complex types to JSON-compatible representations while preserving
 * semantic meaning for CDC consumers.
 */
class ArrowSerializer {
    
    private val objectMapper: ObjectMapper = createObjectMapper()
    
    private fun createObjectMapper(): ObjectMapper {
        val module = SimpleModule("XTDBArrowModule").apply {
            // Temporal types
            addSerializer(Instant::class.java, InstantSerializer())
            addSerializer(LocalDate::class.java, LocalDateSerializer())
            addSerializer(LocalTime::class.java, LocalTimeSerializer())
            addSerializer(LocalDateTime::class.java, LocalDateTimeSerializer())
            addSerializer(OffsetTime::class.java, OffsetTimeSerializer())
            addSerializer(OffsetDateTime::class.java, OffsetDateTimeSerializer())
            addSerializer(ZonedDateTime::class.java, ZonedDateTimeSerializer())
            addSerializer(Duration::class.java, DurationSerializer())
            addSerializer(Period::class.java, PeriodSerializer())
            
            // XTDB-specific types
            addSerializer(Interval::class.java, IntervalSerializer())
            addSerializer(UUID::class.java, UUIDSerializer())
            addSerializer(URI::class.java, URISerializer())
            addSerializer(BigDecimal::class.java, BigDecimalSerializer())
            
            // Binary types
            addSerializer(ByteArray::class.java, ByteArraySerializer())
            
            // Note: Extension types would require access to XTDB's internal extension type classes
            // These are commented out until proper imports are available
            // addSerializer(UuidType::class.java, ExtensionTypeSerializer("uuid"))
            // addSerializer(KeywordType::class.java, ExtensionTypeSerializer("keyword"))
            // addSerializer(TransitType::class.java, ExtensionTypeSerializer("transit"))
            // addSerializer(RegexType::class.java, ExtensionTypeSerializer("regex"))
            // addSerializer(UriType::class.java, ExtensionTypeSerializer("uri"))
        }
        
        return ObjectMapper()
            .registerModule(KotlinModule.Builder().build())
            .registerModule(module)
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
    }
    
    fun serialize(value: Any?): String {
        return objectMapper.writeValueAsString(value)
    }
    
    fun serializeMap(map: Map<String, Any?>): String {
        return objectMapper.writeValueAsString(map)
    }
    
    // Custom serializers for different types
    
    private class InstantSerializer : StdSerializer<Instant>(Instant::class.java) {
        override fun serialize(value: Instant, gen: JsonGenerator, provider: SerializerProvider) {
            gen.writeString(value.toString()) // ISO-8601 format
        }
    }
    
    private class LocalDateSerializer : StdSerializer<LocalDate>(LocalDate::class.java) {
        override fun serialize(value: LocalDate, gen: JsonGenerator, provider: SerializerProvider) {
            gen.writeString(value.toString()) // ISO-8601 format
        }
    }
    
    private class LocalTimeSerializer : StdSerializer<LocalTime>(LocalTime::class.java) {
        override fun serialize(value: LocalTime, gen: JsonGenerator, provider: SerializerProvider) {
            gen.writeString(value.toString()) // ISO-8601 format
        }
    }
    
    private class LocalDateTimeSerializer : StdSerializer<LocalDateTime>(LocalDateTime::class.java) {
        override fun serialize(value: LocalDateTime, gen: JsonGenerator, provider: SerializerProvider) {
            gen.writeString(value.toString()) // ISO-8601 format
        }
    }
    
    private class OffsetTimeSerializer : StdSerializer<OffsetTime>(OffsetTime::class.java) {
        override fun serialize(value: OffsetTime, gen: JsonGenerator, provider: SerializerProvider) {
            gen.writeString(value.toString()) // ISO-8601 format
        }
    }
    
    private class OffsetDateTimeSerializer : StdSerializer<OffsetDateTime>(OffsetDateTime::class.java) {
        override fun serialize(value: OffsetDateTime, gen: JsonGenerator, provider: SerializerProvider) {
            gen.writeString(value.toString()) // ISO-8601 format
        }
    }
    
    private class ZonedDateTimeSerializer : StdSerializer<ZonedDateTime>(ZonedDateTime::class.java) {
        override fun serialize(value: ZonedDateTime, gen: JsonGenerator, provider: SerializerProvider) {
            gen.writeString(value.toString()) // ISO-8601 format
        }
    }
    
    private class DurationSerializer : StdSerializer<Duration>(Duration::class.java) {
        override fun serialize(value: Duration, gen: JsonGenerator, provider: SerializerProvider) {
            // Use ISO-8601 duration format PT...
            gen.writeString(value.toString())
        }
    }
    
    private class PeriodSerializer : StdSerializer<Period>(Period::class.java) {
        override fun serialize(value: Period, gen: JsonGenerator, provider: SerializerProvider) {
            // Use ISO-8601 period format P...
            gen.writeString(value.toString())
        }
    }
    
    private class IntervalSerializer : StdSerializer<Interval>(Interval::class.java) {
        override fun serialize(value: Interval, gen: JsonGenerator, provider: SerializerProvider) {
            gen.writeStartObject()
            gen.writeNumberField("months", value.months)
            gen.writeNumberField("days", value.days)
            gen.writeNumberField("nanos", value.nanos)
            gen.writeEndObject()
        }
    }
    
    private class UUIDSerializer : StdSerializer<UUID>(UUID::class.java) {
        override fun serialize(value: UUID, gen: JsonGenerator, provider: SerializerProvider) {
            gen.writeString(value.toString())
        }
    }
    
    private class URISerializer : StdSerializer<URI>(URI::class.java) {
        override fun serialize(value: URI, gen: JsonGenerator, provider: SerializerProvider) {
            gen.writeString(value.toString())
        }
    }
    
    private class BigDecimalSerializer : StdSerializer<BigDecimal>(BigDecimal::class.java) {
        override fun serialize(value: BigDecimal, gen: JsonGenerator, provider: SerializerProvider) {
            // Use string representation to preserve precision
            gen.writeString(value.toPlainString())
        }
    }
    
    private class ByteArraySerializer : StdSerializer<ByteArray>(ByteArray::class.java) {
        override fun serialize(value: ByteArray, gen: JsonGenerator, provider: SerializerProvider) {
            // Base64 encode binary data
            gen.writeString(java.util.Base64.getEncoder().encodeToString(value))
        }
    }
    
    private class ExtensionTypeSerializer(
        private val typeName: String
    ) : StdSerializer<Any>(Any::class.java) {
        override fun serialize(value: Any, gen: JsonGenerator, provider: SerializerProvider) {
            gen.writeStartObject()
            gen.writeStringField("_xtdb_type", typeName)
            gen.writeStringField("value", value.toString())
            gen.writeEndObject()
        }
    }
}

/**
 * Type-aware value extractor that properly handles Arrow types during
 * CDC processing. Ensures all values are converted to JSON-serializable
 * forms while preserving semantic meaning.
 */
class ArrowValueExtractor(private val serializer: ArrowSerializer = ArrowSerializer()) {
    
    /**
     * Extracts and serializes a value from an Arrow vector, handling all XTDB types.
     */
    fun extractValue(value: Any?): String? {
        return when (value) {
            null -> "null"
            is Map<*, *> -> {
                // Handle nested structures
                val cleanMap = value.entries.associate { (k, v) ->
                    k.toString() to extractObject(v)
                }
                serializer.serializeMap(cleanMap)
            }
            is List<*> -> {
                // Handle arrays/lists
                val cleanList = value.map { extractObject(it) }
                serializer.serialize(cleanList)
            }
            else -> serializer.serialize(extractObject(value))
        }
    }
    
    /**
     * Recursively extracts objects, handling nested structures and Arrow types.
     */
    private fun extractObject(obj: Any?): Any? {
        return when (obj) {
            null -> null
            is Map<*, *> -> obj.entries.associate { (k, v) ->
                k.toString() to extractObject(v)
            }
            is List<*> -> obj.map { extractObject(it) }
            is Set<*> -> obj.map { extractObject(it) }
            is Array<*> -> obj.map { extractObject(it) }
            // Primitive types pass through
            is String, is Number, is Boolean -> obj
            // All other types will be handled by the custom serializers
            else -> obj
        }
    }
}