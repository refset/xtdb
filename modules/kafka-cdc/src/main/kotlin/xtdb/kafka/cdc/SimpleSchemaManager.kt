package xtdb.kafka.cdc

import org.apache.avro.Schema
import xtdb.util.logger

private val LOG = SimpleSchemaManager::class.logger

/**
 * Simplified schema manager that uses JSON strings for all document data.
 * This approach is more robust for XTDB's dynamic schema nature.
 */
class SimpleSchemaManager {
    
    private val schema: Schema by lazy { createEnvelopeSchema() }
    
    fun getSchema(tableName: String): Schema = schema
    
    private fun createEnvelopeSchema(): Schema {
        val schemaString = """
        {
          "type": "record",
          "name": "Envelope",
          "namespace": "xtdb.kafka.cdc",
          "doc": "XTDB CDC envelope with JSON string payloads",
          "fields": [
            {
              "name": "before",
              "type": ["null", "string"],
              "default": null,
              "doc": "State before change as JSON string"
            },
            {
              "name": "after",
              "type": ["null", "string"],
              "default": null,
              "doc": "State after change as JSON string"
            },
            {
              "name": "source",
              "type": {
                "type": "record",
                "name": "Source",
                "fields": [
                  {"name": "version", "type": "string"},
                  {"name": "connector", "type": "string", "default": "xtdb-cdc"},
                  {"name": "name", "type": "string"},
                  {"name": "ts_ms", "type": "long"},
                  {"name": "snapshot", "type": ["null", "string"], "default": null},
                  {"name": "db", "type": "string"},
                  {"name": "table", "type": "string"},
                  {"name": "tx_id", "type": "long"},
                  {"name": "system_time", "type": "long"}
                ]
              }
            },
            {"name": "op", "type": "string"},
            {"name": "ts_ms", "type": ["null", "long"], "default": null},
            {
              "name": "transaction",
              "type": ["null", {
                "type": "record",
                "name": "Transaction",
                "fields": [
                  {"name": "id", "type": "string"},
                  {"name": "total_order", "type": "long"},
                  {"name": "data_collection_order", "type": "long"}
                ]
              }],
              "default": null
            }
          ]
        }
        """.trimIndent()
        
        return Schema.Parser().parse(schemaString)
    }
    
    fun close() {
        // Nothing to close for simple approach
    }
}