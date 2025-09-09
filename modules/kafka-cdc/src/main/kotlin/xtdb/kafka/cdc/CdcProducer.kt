package xtdb.kafka.cdc

import io.confluent.kafka.serializers.KafkaAvroSerializer
import kotlinx.coroutines.future.await
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import xtdb.api.TransactionResult
import xtdb.api.log.MessageId
import xtdb.util.logger
import java.time.Instant
import java.util.Properties
import java.util.concurrent.CompletableFuture
import com.fasterxml.jackson.databind.ObjectMapper

private val LOG = CdcProducer::class.logger

class CdcProducer(
    private val config: CdcConfig
) : CdcProducerInterface {
    
    private val schemaManager = SimpleSchemaManager()
    
    private val producer: KafkaProducer<String, GenericRecord> = createProducer()
    
    private fun createProducer(): KafkaProducer<String, GenericRecord> {
        val props = Properties().apply {
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers)
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer::class.java.name)
            put("schema.registry.url", config.schemaRegistryUrl)
            put(ProducerConfig.ACKS_CONFIG, "all")
            put(ProducerConfig.RETRIES_CONFIG, 3)
            put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true)
            
            // Add any additional properties from config
            config.producerProperties?.forEach { (key, value) ->
                put(key, value)
            }
        }
        
        return KafkaProducer(props)
    }
    
    override suspend fun sendCdcEvent(event: CdcEvent): CompletableFuture<Unit> {
        val schema = schemaManager.getSchema(event.tableName)
        
        val record = createAvroRecord(event, schema)
        val producerRecord = ProducerRecord(
            config.topicPrefix + event.tableName,
            event.recordId,
            record
        )
        
        val future = CompletableFuture<Unit>()
        
        producer.send(producerRecord) { metadata, exception ->
            if (exception != null) {
                // LOG.error("Failed to send CDC event for ${event.tableName}/${event.recordId}", exception)
                future.completeExceptionally(exception)
            } else {
                // LOG.debug("CDC event sent to ${metadata.topic()} partition ${metadata.partition()} offset ${metadata.offset()}")
                future.complete(Unit)
            }
        }
        
        return future
    }
    
    private fun createAvroRecord(event: CdcEvent, schema: Schema): GenericRecord {
        val envelope = GenericData.Record(schema)
        
        // Set before/after based on operation
        when (event.operation) {
            CdcOperation.CREATE -> {
                envelope.put("before", null)
                envelope.put("after", event.afterValue)
            }
            CdcOperation.UPDATE -> {
                envelope.put("before", event.beforeValue)
                envelope.put("after", event.afterValue)
            }
            CdcOperation.DELETE -> {
                envelope.put("before", event.beforeValue)
                envelope.put("after", null)
            }
            CdcOperation.READ -> {
                envelope.put("before", null)
                envelope.put("after", event.afterValue)
            }
        }
        
        // Create source record
        val sourceSchema = schema.getField("source").schema()
        val source = GenericData.Record(sourceSchema)
        source.put("version", config.xtdbVersion)
        source.put("connector", "xtdb-cdc")
        source.put("name", config.nodeName)
        source.put("ts_ms", event.timestamp.toEpochMilli())
        source.put("snapshot", if (event.isSnapshot) "true" else null)
        source.put("db", event.database)
        source.put("table", event.tableName)
        source.put("tx_id", event.txId)
        source.put("system_time", event.systemTime.toEpochMilli())
        
        envelope.put("source", source)
        envelope.put("op", event.operation.code)
        envelope.put("ts_ms", event.timestamp.toEpochMilli())
        
        // Add transaction info if available
        if (event.transactionInfo != null) {
            val txSchema = schema.getField("transaction").schema().types[1] // Get non-null type
            val transaction = GenericData.Record(txSchema)
            transaction.put("id", event.transactionInfo.id)
            transaction.put("total_order", event.transactionInfo.totalOrder)
            transaction.put("data_collection_order", event.transactionInfo.dataCollectionOrder)
            envelope.put("transaction", transaction)
        } else {
            envelope.put("transaction", null)
        }
        
        return envelope
    }
    
    private fun parseJsonToMap(jsonString: String): Map<String, Any?>? {
        return try {
            @Suppress("UNCHECKED_CAST")
            com.fasterxml.jackson.databind.ObjectMapper().readValue(jsonString, Map::class.java) as Map<String, Any?>
        } catch (e: Exception) {
            // LOG.warn("Failed to parse JSON for schema evolution: $jsonString", e)
            null
        }
    }
    
    override fun close() {
        schemaManager.close()
        producer.close()
    }
}

data class CdcConfig(
    val bootstrapServers: String,
    val schemaRegistryUrl: String,
    val topicPrefix: String = "xtdb.cdc.",
    val nodeName: String,
    val xtdbVersion: String = "3.0.0",
    val useJsonStringApproach: Boolean = true,
    val producerProperties: Map<String, Any>? = null
)

data class CdcEvent(
    val database: String,
    val tableName: String,
    val recordId: String,
    val operation: CdcOperation,
    val beforeValue: String?, // JSON string
    val afterValue: String?, // JSON string
    val timestamp: Instant,
    val systemTime: Instant,
    val txId: Long,
    val isSnapshot: Boolean = false,
    val transactionInfo: TransactionInfo? = null,
    // XTDB temporal metadata
    val validFrom: Instant? = null,    // _valid_from - start of valid time
    val validTo: Instant? = null,      // _valid_to - end of valid time  
    val systemFrom: Instant? = null,   // _system_from - when recorded in system
    val systemTo: Instant? = null      // _system_to - when superseded in system
)

data class TransactionInfo(
    val id: String,
    val totalOrder: Long,
    val dataCollectionOrder: Long
)

enum class CdcOperation(val code: String) {
    CREATE("c"),
    UPDATE("u"),
    DELETE("d"),
    READ("r") // for snapshots
}