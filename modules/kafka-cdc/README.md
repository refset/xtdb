# XTDB Kafka CDC Module

This module extends XTDB to write Change Data Capture (CDC) events to Kafka topics in a Debezium-compatible Avro format.

## Features

- **Debezium-Compatible**: Produces CDC events in the standard Debezium envelope format
- **Avro Serialization**: Uses Avro with Schema Registry for efficient, schema-evolution-friendly serialization
- **Transaction Support**: Maintains transaction boundaries and ordering
- **Independent Processing**: CDC runs as a separate subscriber that can lag behind the main indexer
- **Proper Before/After Values**: Queries the database to get accurate before states for updates/deletes
- **Kafka-based Checkpointing**: Uses Kafka topic offsets for checkpoint recovery (no local state)
- **Lag Monitoring**: Built-in metrics to monitor CDC processing lag
- **SQL Support**: Parses SQL statements using XTDB's SQL parser for accurate CDC events
- **Complete Arrow Type Support**: Handles all XTDB Arrow types with proper JSON serialization

## Configuration

Add the following to your XTDB configuration:

```clojure
{:xtdb/node {...}
 
 :xtdb.kafka/cdc
 {:bootstrap-servers "localhost:9092"
  :schema-registry-url "http://localhost:8081"
  :topic-prefix "xtdb.cdc."
  :node-name "xtdb-node-1"
  :lag-threshold "PT30S"
  :enabled? true
  :producer-properties {"compression.type" "snappy"
                        "batch.size" "16384"}}}
```

### Configuration Options

- `bootstrap-servers`: Kafka broker addresses (required)
- `schema-registry-url`: Confluent Schema Registry URL (required)
- `topic-prefix`: Prefix for CDC topic names (default: "xtdb.cdc.")
- `node-name`: Name of the XTDB node for source identification
- `lag-threshold`: Maximum acceptable lag before warnings (default: "PT10S")
- `enabled?`: Enable/disable CDC output (default: true)
- `producer-properties`: Additional Kafka producer properties

## Topic Naming

CDC events are published to topics named: `{topic-prefix}{table-name}`

For example, with the default prefix "xtdb.cdc.", operations on a table called "users" will be published to "xtdb.cdc.users".

## Event Format

Events are published in the Debezium envelope format with the following structure:

```json
{
  "before": {/* Previous state of the record (for updates/deletes) */},
  "after": {/* New state of the record (for creates/updates) */},
  "source": {
    "version": "3.0.0",
    "connector": "xtdb-cdc",
    "name": "xtdb-node-1",
    "ts_ms": 1234567890000,
    "db": "database-name",
    "table": "table-name",
    "tx_id": 12345,
    "system_time": 1234567890000
  },
  "op": "c", // c=create, u=update, d=delete, r=read
  "ts_ms": 1234567890000,
  "transaction": {
    "id": "tx-12345",
    "total_order": 100,
    "data_collection_order": 1
  }
}
```

## Dependencies

Add to your `build.gradle.kts`:

```kotlin
dependencies {
    implementation(project(":modules:xtdb-kafka-cdc"))
}
```

## Usage Example

Once configured, all transactions processed by XTDB will automatically produce CDC events to Kafka:

```clojure
;; This transaction will produce a CDC event to the "xtdb.cdc.users" topic
(xt/submit-tx node
  [[:put-docs :users {:xt/id "user-123" 
                       :name "John Doe"
                       :email "john@example.com"}]])

;; Updates will include the before state
(xt/submit-tx node
  [[:patch-docs :users {:xt/id "user-123" 
                        :email "john.doe@example.com"}]])

;; Deletes will include the before state
(xt/submit-tx node
  [[:delete-docs :users "user-123"]])

;; SQL operations are also captured
(xt/submit-tx node
  [["INSERT INTO users (_id, name, email) VALUES (?, ?, ?)"
    ["user-456" "Jane Smith" "jane@example.com"]]])
```

## Consuming CDC Events

You can consume these events using any Kafka consumer that supports Avro deserialization:

```java
// Java example using Confluent Kafka client
Properties props = new Properties();
props.put("bootstrap.servers", "localhost:9092");
props.put("group.id", "cdc-consumer");
props.put("key.deserializer", StringDeserializer.class.getName());
props.put("value.deserializer", KafkaAvroDeserializer.class.getName());
props.put("schema.registry.url", "http://localhost:8081");

KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(props);
consumer.subscribe(Arrays.asList("xtdb.cdc.users"));

while (true) {
    ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofMillis(100));
    for (ConsumerRecord<String, GenericRecord> record : records) {
        GenericRecord envelope = record.value();
        String operation = envelope.get("op").toString();
        GenericRecord after = (GenericRecord) envelope.get("after");
        // Process the CDC event...
    }
}
```

## Integration with Debezium Consumers

Since the events are Debezium-compatible, you can use any existing Debezium consumer or sink connector:

- Debezium Server
- Kafka Connect JDBC Sink Connector
- Elasticsearch Sink Connector
- Custom Debezium consumers

## Performance Considerations

- CDC runs as an independent processor that doesn't impact transaction latency
- The CDC processor can lag behind the main indexer without affecting write performance
- Failed CDC sends don't fail transactions (errors are logged and metrics recorded)
- No local state management - checkpoints are recovered from Kafka topics
- Consider configuring Kafka producer batching for better throughput
- Monitor the `cdc.lag.ms` metric to ensure CDC keeps up with transaction volume

## Monitoring

The CDC module exposes several metrics:

- `cdc.events.processed`: Counter of successfully processed CDC events
- `cdc.events.failed`: Counter of failed CDC events
- `cdc.lag.ms`: Current CDC processing lag in milliseconds

## Architecture

The CDC module works by:

1. **Startup Recovery**: Reads all CDC topics to find the highest transaction ID processed
2. **Transaction Log Subscription**: Subscribes to XTDB's transaction log from the recovered position
3. **Operation Processing**: Extracts operations (put, patch, delete, SQL) from each transaction
4. **Before State Lookup**: Queries the database for previous values on updates/deletes
5. **Event Creation**: Creates Debezium-compatible Avro events with before/after states
6. **Async Publishing**: Sends events to Kafka topics asynchronously
7. **Stateless Operation**: No local checkpoint files - recovery is entirely from Kafka

## Kafka-based Checkpointing

The CDC processor uses a clever checkpointing mechanism:

- **No Local State**: No checkpoint files or local databases needed
- **Kafka as Source of Truth**: Reads existing CDC topics to determine last processed transaction
- **Single Producer Assumption**: Since only one XTDB node writes to each topic, the highest `tx_id` in any CDC topic represents the last processed transaction
- **Automatic Recovery**: On restart, scans all CDC topics and resumes from the correct position
- **Distributed-Friendly**: Multiple XTDB nodes can run CDC independently without coordination

This approach is more robust than file-based checkpointing because:
- Survives node failures and relocations
- Works in containerized environments
- No shared storage requirements
- Leverages Kafka's durability guarantees

## Arrow Type Support

The CDC module provides comprehensive support for all XTDB Arrow types:

### Temporal Types
- `Instant`: Serialized as ISO-8601 strings (e.g., "2023-12-25T10:15:30.123Z")
- `LocalDate`: Serialized as ISO-8601 dates (e.g., "2023-12-25")
- `LocalTime`: Serialized as ISO-8601 times (e.g., "10:15:30")
- `LocalDateTime`: Serialized as ISO-8601 date-times (e.g., "2023-12-25T10:15:30")
- `OffsetTime`, `OffsetDateTime`, `ZonedDateTime`: Preserved with timezone info
- `Duration`: ISO-8601 duration format (e.g., "PT2H30M")
- `Period`: ISO-8601 period format (e.g., "P1Y6M15D")

### XTDB-Specific Types
- `Interval`: JSON object with `{"months": 13, "days": 45, "nanos": 123456789}`
- `UUID`: String representation (e.g., "550e8400-e29b-41d4-a716-446655440000")
- `URI`: String representation (e.g., "https://example.com/path")
- `BigDecimal`: String to preserve precision (e.g., "123456789.123456789012345")
- `ByteArray`: Base64 encoded strings

### Extension Types
- Keyword types: `{"_xtdb_type": "keyword", "value": "my-keyword"}`
- UUID types: `{"_xtdb_type": "uuid", "value": "550e8400-..."}`
- Transit types: `{"_xtdb_type": "transit", "value": "..."}` 
- Regex types: `{"_xtdb_type": "regex", "value": "pattern"}`
- URI types: `{"_xtdb_type": "uri", "value": "https://..."}`

### Complex Types
- **Maps/Structs**: Nested JSON objects with recursive type handling
- **Arrays/Lists**: JSON arrays with proper element serialization
- **Sets**: Converted to JSON arrays
- **Nested Structures**: Full recursive support for arbitrary nesting

### Example CDC Event with Complex Types

```json
{
  "before": null,
  "after": "{
    \"_id\": \"user-123\",
    \"name\": \"John Doe\",
    \"created_at\": \"2023-12-25T10:15:30.123Z\",
    \"profile\": {
      \"age\": 30,
      \"balance\": \"999.95\",
      \"uuid\": \"550e8400-e29b-41d4-a716-446655440000\",
      \"tags\": [\"premium\", \"verified\"]
    },
    \"subscription_period\": {\"months\": 12, \"days\": 0, \"nanos\": 0}
  }",
  "source": {
    "version": "3.0.0",
    "connector": "xtdb-cdc",
    "name": "xtdb-node-1",
    "ts_ms": 1703505330123,
    "db": "mydb",
    "table": "users",
    "tx_id": 12345,
    "system_time": 1703505330123
  },
  "op": "c",
  "ts_ms": 1703505330123
}
```

This ensures that CDC consumers receive properly typed data that can be accurately reconstructed, making the CDC stream suitable for:
- Data warehousing with full type fidelity
- Real-time analytics with temporal data
- Cross-system integration with complex data structures
- Audit trails with precise timestamps and intervals

## Testing

Run tests with:

```bash
./gradlew :modules:xtdb-kafka-cdc:test
```

Integration tests require Docker for running Kafka containers.