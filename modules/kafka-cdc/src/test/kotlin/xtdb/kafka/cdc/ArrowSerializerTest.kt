package xtdb.kafka.cdc

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import xtdb.time.Interval
import java.math.BigDecimal
import java.net.URI
import java.time.*
import java.util.*

class ArrowSerializerTest {
    
    private val arrowSerializer = ArrowSerializer()
    private val objectMapper = ObjectMapper()
    private val arrowValueExtractor = ArrowValueExtractor(arrowSerializer)
    
    @Test
    fun `test temporal types serialization`() {
        val instant = Instant.parse("2023-12-25T10:15:30.123Z")
        val localDate = LocalDate.of(2023, 12, 25)
        val localTime = LocalTime.of(10, 15, 30)
        val localDateTime = LocalDateTime.of(2023, 12, 25, 10, 15, 30)
        val duration = Duration.ofHours(2).plusMinutes(30)
        val period = Period.ofYears(1).withMonths(6).withDays(15)
        
        assertEquals("\"2023-12-25T10:15:30.123Z\"", arrowSerializer.serialize(instant))
        assertEquals("\"2023-12-25\"", arrowSerializer.serialize(localDate))
        assertEquals("\"10:15:30\"", arrowSerializer.serialize(localTime))
        assertEquals("\"2023-12-25T10:15:30\"", arrowSerializer.serialize(localDateTime))
        assertEquals("\"PT2H30M\"", arrowSerializer.serialize(duration))
        assertEquals("\"P1Y6M15D\"", arrowSerializer.serialize(period))
    }
    
    @Test
    fun `test XTDB interval type`() {
        val interval = Interval(months = 13, days = 45, nanos = 123456789L)
        val json = arrowSerializer.serialize(interval)
        val node = objectMapper.readTree(json)
        
        assertEquals(13, node.get("months").asInt())
        assertEquals(45, node.get("days").asInt())
        assertEquals(123456789L, node.get("nanos").asLong())
    }
    
    @Test
    fun `test UUID serialization`() {
        val uuid = UUID.fromString("550e8400-e29b-41d4-a716-446655440000")
        val json = arrowSerializer.serialize(uuid)
        
        assertEquals("\"550e8400-e29b-41d4-a716-446655440000\"", json)
    }
    
    @Test
    fun `test URI serialization`() {
        val uri = URI.create("https://example.com/path?param=value")
        val json = arrowSerializer.serialize(uri)
        
        assertEquals("\"https://example.com/path?param=value\"", json)
    }
    
    @Test
    fun `test BigDecimal precision preservation`() {
        val bigDecimal = BigDecimal("123456789.123456789012345")
        val json = arrowSerializer.serialize(bigDecimal)
        
        assertEquals("\"123456789.123456789012345\"", json)
    }
    
    @Test
    fun `test binary data encoding`() {
        val binaryData = byteArrayOf(0x00, 0x01, 0x02, 0xFF.toByte(), 0xFE.toByte())
        val json = arrowSerializer.serialize(binaryData)
        val expected = "\"" + Base64.getEncoder().encodeToString(binaryData) + "\""
        
        assertEquals(expected, json)
    }
    
    @Test
    fun `test complex nested structure`() {
        val complexData = mapOf(
            "id" to UUID.fromString("550e8400-e29b-41d4-a716-446655440000"),
            "timestamp" to Instant.parse("2023-12-25T10:15:30.123Z"),
            "duration" to Duration.ofMinutes(45),
            "metadata" to mapOf(
                "tags" to listOf("important", "urgent"),
                "score" to BigDecimal("99.95"),
                "binary" to byteArrayOf(0x01, 0x02, 0x03)
            ),
            "interval" to Interval(months = 6, days = 15, nanos = 0L)
        )
        
        val json = arrowValueExtractor.extractValue(complexData)
        assertNotNull(json)
        
        val node = objectMapper.readTree(json)
        assertEquals("550e8400-e29b-41d4-a716-446655440000", node.get("id").asText())
        assertEquals("2023-12-25T10:15:30.123Z", node.get("timestamp").asText())
        assertEquals("PT45M", node.get("duration").asText())
        assertEquals("99.95", node.get("metadata").get("score").asText())
        assertEquals(6, node.get("interval").get("months").asInt())
    }
    
    @Test
    fun `test null values handling`() {
        val dataWithNulls = mapOf(
            "field1" to "value",
            "field2" to null,
            "field3" to listOf("a", null, "c"),
            "field4" to mapOf("nested" to null)
        )
        
        val json = arrowValueExtractor.extractValue(dataWithNulls)
        assertNotNull(json)
        
        val node = objectMapper.readTree(json)
        assertEquals("value", node.get("field1").asText())
        assertTrue(node.get("field2").isNull)
        assertTrue(node.get("field3").get(1).isNull)
        assertTrue(node.get("field4").get("nested").isNull)
    }
    
    @Test
    fun `test array types`() {
        val arrayData = listOf(
            UUID.fromString("550e8400-e29b-41d4-a716-446655440000"),
            Instant.parse("2023-12-25T10:15:30.123Z"),
            Duration.ofHours(1),
            BigDecimal("123.456")
        )
        
        val json = arrowValueExtractor.extractValue(arrayData)
        assertNotNull(json)
        
        val node = objectMapper.readTree(json)
        assertTrue(node.isArray)
        assertEquals(4, node.size())
        assertEquals("550e8400-e29b-41d4-a716-446655440000", node.get(0).asText())
        assertEquals("2023-12-25T10:15:30.123Z", node.get(1).asText())
        assertEquals("PT1H", node.get(2).asText())
        assertEquals("123.456", node.get(3).asText())
    }
    
    @Test
    fun `test empty and special values`() {
        assertEquals("null", arrowValueExtractor.extractValue(null))
        assertEquals("\"\"", arrowSerializer.serialize(""))
        assertEquals("[]", arrowSerializer.serialize(emptyList<Any>()))
        assertEquals("{}", arrowSerializer.serialize(emptyMap<String, Any>()))
    }
    
    @Test
    fun `test primitive types pass through`() {
        assertEquals("42", arrowSerializer.serialize(42))
        assertEquals("3.14", arrowSerializer.serialize(3.14))
        assertEquals("true", arrowSerializer.serialize(true))
        assertEquals("\"hello\"", arrowSerializer.serialize("hello"))
    }
}