package xtdb.kafka.cdc

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.module.kotlin.KotlinModule
import org.apache.avro.generic.GenericRecord
import java.io.File
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardOpenOption
import java.time.Instant
import java.time.format.DateTimeFormatter
import java.util.concurrent.atomic.AtomicLong

/**
 * File-based CDC writer for testing and inspection.
 * Writes CDC events as JSON files to a directory structure.
 */
class FileCdcWriter(
    private val outputDir: Path = Paths.get("cdc-output"),
    private val prettyPrint: Boolean = true
) : AutoCloseable {
    
    private val objectMapper = ObjectMapper()
        .registerModule(KotlinModule.Builder().build())
        .apply {
            if (prettyPrint) {
                enable(SerializationFeature.INDENT_OUTPUT)
            }
            disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
        }
    
    private val eventCounter = AtomicLong(0)
    private val schemaManager = SimpleSchemaManager()
    
    init {
        Files.createDirectories(outputDir)
        println("CDC File Writer initialized. Output directory: ${outputDir.toAbsolutePath()}")
    }
    
    fun writeCdcEvent(event: CdcEvent) {
        val tableDir = outputDir.resolve(event.tableName)
        Files.createDirectories(tableDir)
        
        val eventNumber = eventCounter.incrementAndGet()
        val timestamp = DateTimeFormatter.ISO_INSTANT.format(event.timestamp)
            .replace(':', '-').replace('.', '-')
        
        // Create filename based on operation and timestamp
        val filename = "${eventNumber.toString().padStart(6, '0')}_${event.operation.code}_${event.recordId}_${timestamp}.json"
        val eventFile = tableDir.resolve(filename)
        
        // Convert to Debezium-compatible format
        val cdcJson = createDebeziumJson(event)
        
        Files.write(
            eventFile, 
            objectMapper.writeValueAsBytes(cdcJson),
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.TRUNCATE_EXISTING
        )
        
        println("Wrote CDC event to: ${eventFile.fileName}")
    }
    
    private fun createDebeziumJson(event: CdcEvent): Map<String, Any?> {
        // Parse the JSON strings back to objects for pretty printing
        val beforeObj = event.beforeValue?.let { 
            try {
                val parsed = objectMapper.readValue(it, Map::class.java) as MutableMap<String, Any?>
                addTemporalColumns(parsed, event, isAfterState = false)
                parsed
            } catch (e: Exception) {
                // If not valid JSON, use as string
                event.beforeValue
            }
        }
        
        val afterObj = event.afterValue?.let { 
            try {
                val parsed = objectMapper.readValue(it, Map::class.java) as MutableMap<String, Any?>
                addTemporalColumns(parsed, event, isAfterState = true)
                parsed
            } catch (e: Exception) {
                // If not valid JSON, use as string
                event.afterValue
            }
        }
        
        return mapOf(
            "schema" to mapOf(
                "type" to "struct",
                "name" to "xtdb.kafka.cdc.Envelope",
                "optional" to false,
                "fields" to listOf(
                    mapOf("field" to "before", "optional" to true),
                    mapOf("field" to "after", "optional" to true),
                    mapOf("field" to "source", "optional" to false),
                    mapOf("field" to "op", "optional" to false),
                    mapOf("field" to "ts_ms", "optional" to true),
                    mapOf("field" to "transaction", "optional" to true)
                )
            ),
            "payload" to mapOf(
                "before" to beforeObj,
                "after" to afterObj,
                "source" to mapOf(
                    "version" to "3.0.0",
                    "connector" to "xtdb-cdc",
                    "name" to event.database,
                    "ts_ms" to event.timestamp.toEpochMilli(),
                    "snapshot" to if (event.isSnapshot) "true" else "false",
                    "db" to event.database,
                    "table" to event.tableName,
                    "tx_id" to event.txId,
                    "system_time" to event.systemTime.toEpochMilli()
                ),
                "op" to event.operation.code,
                "ts_ms" to event.timestamp.toEpochMilli(),
                "transaction" to event.transactionInfo?.let { tx ->
                    mapOf(
                        "id" to tx.id,
                        "total_order" to tx.totalOrder,
                        "data_collection_order" to tx.dataCollectionOrder
                    )
                }
            )
        )
    }
    
    /**
     * Adds XTDB temporal columns to a document.
     * These columns track both valid time (business time) and system time (transaction time).
     */
    private fun addTemporalColumns(
        doc: MutableMap<String, Any?>, 
        event: CdcEvent, 
        isAfterState: Boolean
    ) {
        // Don't add temporal columns if they already exist (user might have set them explicitly)
        if (doc.containsKey("_valid_from")) return
        
        val maxTime = "9999-12-31T23:59:59.999999Z"
        val eventTimeStr = event.timestamp.toString()
        
        when (event.operation) {
            CdcOperation.CREATE -> {
                if (isAfterState) {
                    doc["_valid_from"] = eventTimeStr
                    doc["_valid_to"] = maxTime
                    doc["_system_from"] = eventTimeStr
                    doc["_system_to"] = maxTime
                }
            }
            CdcOperation.UPDATE -> {
                if (isAfterState) {
                    // New version: starts at update time, ends at max time
                    doc["_valid_from"] = eventTimeStr
                    doc["_valid_to"] = maxTime
                    doc["_system_from"] = eventTimeStr
                    doc["_system_to"] = maxTime
                } else {
                    // Previous version: we need to end it at update time
                    // Keep existing _valid_from and _system_from, set end times to update time
                    if (!doc.containsKey("_valid_to")) doc["_valid_to"] = eventTimeStr
                    if (!doc.containsKey("_system_to")) doc["_system_to"] = eventTimeStr
                }
            }
            CdcOperation.DELETE -> {
                if (!isAfterState) {
                    // Deleted version: end both valid and system time at deletion time
                    if (!doc.containsKey("_valid_to")) doc["_valid_to"] = eventTimeStr
                    if (!doc.containsKey("_system_to")) doc["_system_to"] = eventTimeStr
                }
            }
            CdcOperation.READ -> {
                if (isAfterState) {
                    // Snapshot read: use current time as system time
                    doc["_valid_from"] = doc["_valid_from"] ?: eventTimeStr
                    doc["_valid_to"] = doc["_valid_to"] ?: maxTime
                    doc["_system_from"] = eventTimeStr
                    doc["_system_to"] = maxTime
                }
            }
        }
    }
    
    fun writeSummary() {
        val summaryFile = outputDir.resolve("cdc-summary.txt")
        val tables = Files.list(outputDir)
            .filter { Files.isDirectory(it) }
            .map { it.fileName.toString() }
            .sorted()
            .toList()
        
        val summary = buildString {
            appendLine("CDC Output Summary")
            appendLine("==================")
            appendLine("Total events: ${eventCounter.get()}")
            appendLine("Output directory: ${outputDir.toAbsolutePath()}")
            appendLine()
            appendLine("Tables with CDC events:")
            tables.forEach { table ->
                val tableDir = outputDir.resolve(table)
                val eventCount = Files.list(tableDir).count()
                appendLine("  - $table: $eventCount events")
            }
            appendLine()
            appendLine("Generated at: ${Instant.now()}")
        }
        
        Files.write(summaryFile, summary.toByteArray())
        println("\nSummary written to: $summaryFile")
    }
    
    override fun close() {
        writeSummary()
    }
}