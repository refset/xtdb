# Schema Evolution in XTDB Kafka CDC

This document explains how the XTDB Kafka CDC module handles Avro schema evolution when new document shapes are inserted into XTDB.

## The Challenge

XTDB is schemaless and allows arbitrary document structures:

```clojure
;; Day 1: Simple user document
(xt/submit-tx node
  [[:put-docs :users {:xt/id "user-1" 
                      :name "John" 
                      :email "john@example.com"}]])

;; Day 30: Much more complex user document  
(xt/submit-tx node
  [[:put-docs :users {:xt/id "user-2"
                      :name "Jane"
                      :email "jane@example.com"
                      :profile {:bio "Engineer"
                                :skills ["Kotlin" "Clojure"]
                                :certifications #{:aws :k8s}}
                      :preferences {:notifications {:email true :sms false}
                                    :privacy {:public-profile false}}
                      :metadata {:created-at #inst "2023-12-25"
                                 :last-login #inst "2023-12-26"
                                 :session-data (byte-array [1 2 3 4])}}]])
```

But Avro requires predefined schemas. How do we handle this?

## Two Approaches

### 1. JSON String Approach (Recommended)

**How it works:**
- Store document data as JSON strings in Avro
- Use a fixed envelope schema for all tables
- No schema evolution needed

**Avro Schema:**
```json
{
  "type": "record",
  "name": "Envelope", 
  "fields": [
    {
      "name": "before",
      "type": ["null", "string"],
      "doc": "Previous state as JSON string"
    },
    {
      "name": "after", 
      "type": ["null", "string"],
      "doc": "Current state as JSON string"
    },
    // ... source, op, transaction fields
  ]
}
```

**Example CDC Event:**
```json
{
  "before": null,
  "after": "{\"_id\":\"user-2\",\"name\":\"Jane\",\"email\":\"jane@example.com\",\"profile\":{\"bio\":\"Engineer\",\"skills\":[\"Kotlin\",\"Clojure\"]}}",
  "source": { ... },
  "op": "c"
}
```

**Pros:**
- ✅ Handles any document shape without schema changes
- ✅ No Schema Registry complexity  
- ✅ Consumer can parse JSON to get full document structure
- ✅ Works with existing Debezium tooling
- ✅ Simple and reliable

**Cons:**
- ❌ Consumers must parse JSON strings
- ❌ Less efficient than native Avro fields
- ❌ No schema-level field validation

### 2. Dynamic Schema Approach (Advanced)

**How it works:**
- Analyze document structure and generate Avro schemas
- Register new schema versions when document shape changes
- Handle schema evolution and compatibility

**Example Evolution:**

**Version 1 Schema (Simple User):**
```json
{
  "type": "record",
  "name": "Envelope",
  "fields": [
    {
      "name": "after",
      "type": ["null", {
        "type": "record", 
        "name": "User",
        "fields": [
          {"name": "_id", "type": "string"},
          {"name": "name", "type": "string"},
          {"name": "email", "type": "string"}
        ]
      }]
    }
    // ... other envelope fields
  ]
}
```

**Version 2 Schema (Complex User):**
```json
{
  "type": "record",
  "name": "Envelope", 
  "fields": [
    {
      "name": "after",
      "type": ["null", {
        "type": "record",
        "name": "User", 
        "fields": [
          {"name": "_id", "type": "string"},
          {"name": "name", "type": "string"},
          {"name": "email", "type": "string"},
          {"name": "profile", "type": ["null", {
            "type": "record",
            "name": "Profile",
            "fields": [
              {"name": "bio", "type": ["null", "string"]},
              {"name": "skills", "type": ["null", {"type": "array", "items": "string"}]}
            ]
          }], "default": null}
        ]
      }]
    }
  ]
}
```

**Pros:**
- ✅ Native Avro field access
- ✅ Better performance for consumers
- ✅ Schema-level validation
- ✅ Schema Registry integration

**Cons:**
- ❌ Complex schema evolution logic
- ❌ May require schema registry coordination
- ❌ Handling of incompatible changes is tricky
- ❌ Some XTDB types don't map cleanly to Avro

## Configuration

Choose your approach in the XTDB configuration:

```clojure
{:xtdb.kafka/cdc
 {:bootstrap-servers "localhost:9092"
  :schema-registry-url "http://localhost:8081"
  :use-json-string-approach? true    ;; true = JSON strings, false = dynamic schemas
  :enabled? true}}
```

## JSON String Approach Deep Dive

This is the **recommended approach** for most use cases.

### Schema Structure

All tables use the same envelope schema with JSON string payloads:

```
Topic: xtdb.cdc.users
Schema: Envelope (fixed)
Message Key: record ID
Message Value: {
  "before": "...",    // JSON string or null
  "after": "...",     // JSON string or null  
  "source": { ... },
  "op": "c|u|d",
  "transaction": { ... }
}
```

### Type Handling in JSON

XTDB's Arrow types are serialized to JSON with semantic preservation:

```json
{
  "_id": "user-123",
  "created_at": "2023-12-25T10:15:30.123Z",        // Instant → ISO-8601
  "duration": "PT2H30M",                           // Duration → ISO-8601
  "period": "P1Y6M15D",                           // Period → ISO-8601
  "balance": "999.95",                            // BigDecimal → string
  "uuid": "550e8400-e29b-41d4-a716-446655440000", // UUID → string
  "binary_data": "AQID/w==",                       // ByteArray → Base64
  "interval": {"months": 6, "days": 15, "nanos": 0}, // Interval → object
  "tags": ["premium", "verified"],                 // List → array
  "metadata": {                                    // Nested object
    "source": "web",
    "session": "sess-123"
  }
}
```

### Consumer Examples

**Kafka Streams (Kotlin):**
```kotlin
val stream: KStream<String, GenericRecord> = builder.stream("xtdb.cdc.users")

stream.mapValues { envelope ->
    val afterJson = envelope.get("after") as String?
    if (afterJson != null) {
        val objectMapper = ObjectMapper()
        val user = objectMapper.readValue(afterJson, Map::class.java)
        // Process user data...
        user
    } else {
        null // Deleted record
    }
}
```

**Apache Flink (Scala):**
```scala
val stream = env.addSource(
  new FlinkKafkaConsumer("xtdb.cdc.users", new GenericRecordSchema(), properties)
)

stream.map { envelope =>
  val after = envelope.get("after").toString
  if (after != "null") {
    val json = parse(after)  // Using your favorite JSON library
    // Process JSON...
  }
}
```

**Python Consumer:**
```python
from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'xtdb.cdc.users',
    value_deserializer=lambda m: avro.loads(m)  # Avro deserializer
)

for message in consumer:
    envelope = message.value
    after_json = envelope['after']
    
    if after_json:
        user_data = json.loads(after_json)
        # Process user_data dict...
```

## Dynamic Schema Approach Deep Dive

This approach is more complex but provides native Avro benefits.

### Schema Evolution Process

1. **New Document Shape Detected:**
   ```kotlin
   val newUser = mapOf(
       "_id" to "user-123",
       "name" to "John",
       "age" to 30,
       "tags" to listOf("premium")  // New field!
   )
   ```

2. **Schema Analysis:**
   - Current schema only has `_id`, `name` fields
   - New document adds `age` (int) and `tags` (array of strings)
   - Need to evolve schema

3. **Compatibility Check:**
   - Adding optional fields = FORWARD compatible
   - Changing field types = BREAKING change
   - Removing fields = BACKWARD incompatible

4. **Schema Registration:**
   ```kotlin
   val evolvedSchema = SchemaBuilder
       .record("User")
       .fields()
       .name("_id").type().stringType().noDefault()
       .name("name").type().stringType().noDefault()
       .name("age").type().nullable().intType().noDefault()        // New optional field
       .name("tags").type().nullable().array().items().stringType().noDefault()  // New optional field
       .endRecord()
   
   val schemaId = schemaRegistry.register("xtdb.cdc.users-value", evolvedSchema)
   ```

5. **Event Production:**
   - Use new schema for serialization
   - Consumers can read with forward/backward compatibility rules

### Handling Complex Cases

**Type Conflicts:**
```kotlin
// Document 1: age is int
{"_id": "user-1", "age": 25}

// Document 2: age is string  
{"_id": "user-2", "age": "twenty-five"}
```

Resolution: Fall back to JSON string approach for this field.

**Deep Nesting:**
```kotlin
{"profile": {"preferences": {"notifications": {"email": {"enabled": true, "frequency": "daily"}}}}}
```

Resolution: Generate nested record schemas up to a depth limit, then use JSON strings.

## Best Practices

### 1. Use JSON String Approach Initially

Start with JSON strings unless you have specific requirements for native Avro fields:

```clojure
{:xtdb.kafka/cdc
 {:use-json-string-approach? true}}  ;; Start here
```

### 2. Monitor Schema Registry Growth

If using dynamic schemas:

```bash
# Check number of schema versions
curl http://localhost:8081/subjects/

# Monitor schema evolution
curl http://localhost:8081/subjects/xtdb.cdc.users-value/versions
```

### 3. Consumer Compatibility Strategy

Design consumers to handle both approaches:

```kotlin
fun processEnvelope(envelope: GenericRecord): UserData? {
    val after = envelope.get("after")
    
    return when (after) {
        is String -> {
            // JSON string approach
            objectMapper.readValue(after, UserData::class.java)
        }
        is GenericRecord -> {
            // Dynamic schema approach  
            convertFromAvroRecord(after)
        }
        null -> null  // Deleted record
        else -> throw IllegalArgumentException("Unexpected after type: ${after::class}")
    }
}
```

### 4. Testing Schema Evolution

Test your consumers with various document shapes:

```kotlin
@Test
fun `handles schema evolution gracefully`() {
    // Test with simple document
    val simpleUser = createCdcEvent(mapOf("_id" to "1", "name" to "John"))
    consumer.process(simpleUser)
    
    // Test with complex document
    val complexUser = createCdcEvent(mapOf(
        "_id" to "2", 
        "name" to "Jane",
        "profile" to mapOf("bio" to "Engineer", "skills" to listOf("Kotlin"))
    ))
    consumer.process(complexUser)
    
    // Both should work without consumer changes
}
```

## Migration Strategy

If you need to switch from JSON string to dynamic schemas:

1. **Dual Writing Period:**
   - Write events in both formats to different topics
   - `xtdb.cdc.users` (JSON string) and `xtdb.cdc.users.typed` (dynamic)

2. **Consumer Migration:**
   - Update consumers to read from new topics
   - Test thoroughly with production data shapes

3. **Cutover:**
   - Switch CDC to dynamic approach
   - Retire old topics after retention period

## Troubleshooting

### Schema Registry Issues

```bash
# Check schema registry health
curl http://localhost:8081/config

# List all subjects
curl http://localhost:8081/subjects

# Get latest schema for topic
curl http://localhost:8081/subjects/xtdb.cdc.users-value/versions/latest
```

### Consumer Deserialization Errors

Most commonly caused by:
1. Schema evolution incompatibilities
2. Missing Schema Registry connectivity
3. Consumer using wrong deserializer configuration

### CDC Lag Due to Schema Evolution

Monitor CDC processing lag when using dynamic schemas:
- Schema inference can be expensive for complex documents
- Schema registry calls add latency
- Consider batching schema operations

## Summary

- **JSON String Approach**: Simple, reliable, handles any XTDB document shape
- **Dynamic Schema Approach**: More complex but provides native Avro benefits
- **Recommendation**: Start with JSON strings, migrate to dynamic schemas only if needed
- **Both approaches**: Preserve full XTDB type information and maintain Debezium compatibility

The CDC module handles XTDB's flexible schema nature while providing reliable, evolvable integration with the Kafka ecosystem.