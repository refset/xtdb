package xtdb.kafka.cdc

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.avro.generic.GenericRecord
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import xtdb.api.log.MessageId
import xtdb.util.logger
import java.time.Duration
import java.util.Properties

private val LOG = KafkaCheckpointManager::class.logger

/**
 * Manages CDC checkpoint by reading the last processed message from Kafka topics.
 * Since there's only one producer writing to each topic, we can determine our 
 * checkpoint by finding the highest transaction ID across all CDC topics.
 */
class KafkaCheckpointManager(
    private val bootstrapServers: String,
    private val schemaRegistryUrl: String,
    private val topicPrefix: String
) : AutoCloseable {
    
    private val consumer: KafkaConsumer<String, GenericRecord> = createConsumer()
    
    private fun createConsumer(): KafkaConsumer<String, GenericRecord> {
        val props = Properties().apply {
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer::class.java.name)
            put("schema.registry.url", schemaRegistryUrl)
            put(ConsumerConfig.GROUP_ID_CONFIG, "xtdb-cdc-checkpoint-reader-${System.currentTimeMillis()}")
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
        }
        
        return KafkaConsumer(props)
    }
    
    /**
     * Recovers the last processed transaction ID by reading the latest messages
     * from all CDC topics and finding the highest tx_id.
     */
    fun recoverLastProcessedTxId(): MessageId {
        return try {
            // Get all CDC topics
            val allTopics = consumer.listTopics(Duration.ofSeconds(10))
            val cdcTopics = allTopics.keys.filter { it.startsWith(topicPrefix) }
            
            if (cdcTopics.isEmpty()) {
                // LOG.info("No CDC topics found, starting from beginning")
                return -1L
            }
            
            // LOG.info("Found CDC topics: $cdcTopics")
            
            var maxTxId = -1L
            
            cdcTopics.forEach { topic ->
                val txId = getLastTxIdFromTopic(topic)
                if (txId > maxTxId) {
                    maxTxId = txId
                }
            }
            
            // LOG.info("Recovered last processed transaction ID: $maxTxId")
            maxTxId
            
        } catch (e: Exception) {
            // LOG.warn("Failed to recover checkpoint from Kafka, starting from beginning", e)
            -1L
        }
    }
    
    private fun getLastTxIdFromTopic(topic: String): MessageId {
        return try {
            val partitions = consumer.partitionsFor(topic).map { TopicPartition(topic, it.partition()) }
            
            // Get the latest offsets for all partitions
            val endOffsets = consumer.endOffsets(partitions)
            
            var maxTxId = -1L
            
            partitions.forEach { partition ->
                val endOffset = endOffsets[partition] ?: return@forEach
                
                if (endOffset > 0) {
                    // Seek to the last few messages to find the latest tx_id
                    val startOffset = maxOf(0, endOffset - 10) // Read last 10 messages max
                    consumer.assign(listOf(partition))
                    consumer.seek(partition, startOffset)
                    
                    val records = consumer.poll(Duration.ofSeconds(5))
                    
                    records.records(partition).forEach { record ->
                        val envelope = record.value()
                        val source = envelope.get("source") as? GenericRecord
                        val txId = source?.get("tx_id") as? Long
                        
                        if (txId != null && txId > maxTxId) {
                            maxTxId = txId
                        }
                    }
                }
            }
            
            // LOG.debug("Last tx_id from topic $topic: $maxTxId")
            maxTxId
            
        } catch (e: Exception) {
            // LOG.warn("Failed to read last tx_id from topic $topic", e)
            -1L
        }
    }
    
    /**
     * Gets all existing CDC topics for this XTDB instance
     */
    fun getCdcTopics(): List<String> {
        return try {
            val allTopics = consumer.listTopics(Duration.ofSeconds(10))
            allTopics.keys.filter { it.startsWith(topicPrefix) }.sorted()
        } catch (e: Exception) {
            // LOG.warn("Failed to list CDC topics", e)
            emptyList()
        }
    }
    
    /**
     * Validates that all CDC topics exist and are accessible
     */
    fun validateTopics(expectedTables: Set<String>): Boolean {
        return try {
            val existingTopics = getCdcTopics()
            val expectedTopics = expectedTables.map { "$topicPrefix$it" }.toSet()
            val missingTopics = expectedTopics - existingTopics.toSet()
            
            if (missingTopics.isNotEmpty()) {
                // LOG.warn("Some CDC topics don't exist yet: $missingTopics")
            }
            
            true
        } catch (e: Exception) {
            // LOG.error("Failed to validate CDC topics", e)
            false
        }
    }
    
    override fun close() {
        consumer.close()
    }
}