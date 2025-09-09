#!/bin/bash

# Simple script to generate CDC example files

echo "Generating CDC example files..."

# Create a simple Kotlin script to generate the files
cat > /tmp/cdc-generator.kt << 'EOF'
import java.nio.file.*
import java.time.Instant
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature

val outputDir = Paths.get("cdc-output-example")
Files.createDirectories(outputDir)

val mapper = ObjectMapper().apply {
    enable(SerializationFeature.INDENT_OUTPUT)
}

fun writeEvent(table: String, op: String, recordId: String, before: Map<String, Any?>?, after: Map<String, Any?>?) {
    val tableDir = outputDir.resolve(table)
    Files.createDirectories(tableDir)
    
    val timestamp = Instant.now()
    val filename = "${op}_${recordId}_${System.currentTimeMillis()}.json"
    val file = tableDir.resolve(filename)
    
    val event = mapOf(
        "schema" to mapOf(
            "type" to "struct",
            "name" to "xtdb.kafka.cdc.Envelope"
        ),
        "payload" to mapOf(
            "before" to before,
            "after" to after,
            "source" to mapOf(
                "version" to "3.0.0",
                "connector" to "xtdb-cdc",
                "name" to "xtdb",
                "ts_ms" to timestamp.toEpochMilli(),
                "db" to "xtdb",
                "table" to table,
                "tx_id" to System.currentTimeMillis()
            ),
            "op" to op,
            "ts_ms" to timestamp.toEpochMilli()
        )
    )
    
    Files.write(file, mapper.writeValueAsBytes(event))
    println("Created: $file")
}

// CREATE event - new user
writeEvent(
    "users", 
    "c",
    "user-123",
    null,
    mapOf(
        "_id" to "user-123",
        "name" to "Alice Smith",
        "email" to "alice@example.com",
        "created_at" to Instant.now().toString()
    )
)

// UPDATE event
writeEvent(
    "users",
    "u", 
    "user-123",
    mapOf(
        "_id" to "user-123",
        "name" to "Alice Smith",
        "email" to "alice@example.com"
    ),
    mapOf(
        "_id" to "user-123",
        "name" to "Alice Johnson",
        "email" to "alice.johnson@example.com",
        "updated_at" to Instant.now().toString()
    )
)

// DELETE event
writeEvent(
    "users",
    "d",
    "user-999",
    mapOf(
        "_id" to "user-999",
        "name" to "Bob Jones",
        "email" to "bob@example.com"
    ),
    null
)

// Product CREATE
writeEvent(
    "products",
    "c",
    "prod-456",
    null,
    mapOf(
        "_id" to "prod-456",
        "name" to "Widget Pro",
        "price" to 29.99,
        "category" to "widgets",
        "in_stock" to true,
        "tags" to listOf("premium", "bestseller")
    )
)

// Order with nested structure
writeEvent(
    "orders",
    "c",
    "order-789",
    null,
    mapOf(
        "_id" to "order-789",
        "customer_id" to "user-123",
        "items" to listOf(
            mapOf("product_id" to "prod-456", "quantity" to 2, "price" to 29.99),
            mapOf("product_id" to "prod-789", "quantity" to 1, "price" to 49.99)
        ),
        "total" to 109.97,
        "status" to "pending",
        "shipping_address" to mapOf(
            "street" to "123 Main St",
            "city" to "Springfield",
            "zip" to "12345"
        )
    )
)

println("\n✅ CDC example files created in: ${outputDir.toAbsolutePath()}")
println("\nDirectory structure:")
Files.list(outputDir).forEach { dir ->
    if (Files.isDirectory(dir)) {
        val count = Files.list(dir).count()
        println("  ${dir.fileName}/ - $count events")
    }
}
EOF

# For now, let's just create the example files directly with simple JSON
mkdir -p cdc-output-example/{users,products,orders}

# User CREATE event
cat > cdc-output-example/users/001_c_user-123.json << 'EOF'
{
  "schema": {
    "type": "struct",
    "name": "xtdb.kafka.cdc.Envelope",
    "fields": [
      {"field": "before", "optional": true},
      {"field": "after", "optional": true},
      {"field": "source", "optional": false},
      {"field": "op", "optional": false},
      {"field": "ts_ms", "optional": true}
    ]
  },
  "payload": {
    "before": null,
    "after": {
      "_id": "user-123",
      "_valid_from": "2024-01-15T10:30:00Z",
      "_valid_to": "9999-12-31T23:59:59.999999Z",
      "_system_from": "2024-01-15T10:30:00Z",
      "_system_to": "9999-12-31T23:59:59.999999Z",
      "name": "Alice Smith",
      "email": "alice@example.com",
      "created_at": "2024-01-15T10:30:00Z",
      "status": "active",
      "preferences": {
        "theme": "dark",
        "notifications": true
      }
    },
    "source": {
      "version": "3.0.0",
      "connector": "xtdb-cdc",
      "name": "xtdb",
      "ts_ms": 1705316400000,
      "db": "xtdb",
      "table": "users",
      "tx_id": 1001,
      "system_time": 1705316400000
    },
    "op": "c",
    "ts_ms": 1705316400000
  }
}
EOF

# User UPDATE event
cat > cdc-output-example/users/002_u_user-123.json << 'EOF'
{
  "schema": {
    "type": "struct",
    "name": "xtdb.kafka.cdc.Envelope"
  },
  "payload": {
    "before": {
      "_id": "user-123",
      "_valid_from": "2024-01-15T10:30:00Z",
      "_valid_to": "2024-01-15T14:30:00Z",
      "_system_from": "2024-01-15T10:30:00Z",
      "_system_to": "2024-01-15T14:30:00Z",
      "name": "Alice Smith",
      "email": "alice@example.com",
      "created_at": "2024-01-15T10:30:00Z",
      "status": "active"
    },
    "after": {
      "_id": "user-123",
      "_valid_from": "2024-01-15T14:30:00Z",
      "_valid_to": "9999-12-31T23:59:59.999999Z",
      "_system_from": "2024-01-15T14:30:00Z",
      "_system_to": "9999-12-31T23:59:59.999999Z",
      "name": "Alice Johnson-Smith",
      "email": "alice.johnson@example.com",
      "created_at": "2024-01-15T10:30:00Z",
      "updated_at": "2024-01-15T14:30:00Z",
      "status": "active",
      "bio": "Software engineer",
      "preferences": {
        "theme": "dark",
        "notifications": true,
        "language": "en"
      }
    },
    "source": {
      "version": "3.0.0",
      "connector": "xtdb-cdc",
      "name": "xtdb",
      "ts_ms": 1705330800000,
      "db": "xtdb",
      "table": "users",
      "tx_id": 1002
    },
    "op": "u",
    "ts_ms": 1705330800000
  }
}
EOF

# User DELETE event
cat > cdc-output-example/users/003_d_user-999.json << 'EOF'
{
  "schema": {
    "type": "struct",
    "name": "xtdb.kafka.cdc.Envelope"
  },
  "payload": {
    "before": {
      "_id": "user-999",
      "_valid_from": "2024-01-10T09:00:00Z",
      "_valid_to": "2024-01-15T16:00:00Z",
      "_system_from": "2024-01-10T09:00:00Z",
      "_system_to": "2024-01-15T16:00:00Z",
      "name": "Bob Jones",
      "email": "bob@example.com",
      "status": "inactive"
    },
    "after": null,
    "source": {
      "version": "3.0.0",
      "connector": "xtdb-cdc",
      "name": "xtdb",
      "ts_ms": 1705334400000,
      "db": "xtdb",
      "table": "users",
      "tx_id": 1003
    },
    "op": "d",
    "ts_ms": 1705334400000
  }
}
EOF

# Product CREATE
cat > cdc-output-example/products/001_c_prod-456.json << 'EOF'
{
  "schema": {
    "type": "struct",
    "name": "xtdb.kafka.cdc.Envelope"
  },
  "payload": {
    "before": null,
    "after": {
      "_id": "prod-456",
      "_valid_from": "2024-01-15T09:00:00Z",
      "_valid_to": "9999-12-31T23:59:59.999999Z",
      "_system_from": "2024-01-15T09:00:00Z",
      "_system_to": "9999-12-31T23:59:59.999999Z",
      "sku": "WGT-PRO-2024",
      "name": "Widget Pro",
      "description": "Professional grade widget with advanced features",
      "price": 29.99,
      "price_decimal": "29.99",
      "category": "widgets",
      "subcategory": "professional",
      "in_stock": true,
      "quantity": 150,
      "warehouse_location": "A-12-3",
      "tags": ["premium", "bestseller", "2024"],
      "specifications": {
        "weight_kg": 0.5,
        "dimensions_cm": [10, 10, 5],
        "color": "metallic silver",
        "warranty_months": 24
      },
      "created_at": "2024-01-15T09:00:00Z",
      "last_restocked": "2024-01-10T14:00:00Z"
    },
    "source": {
      "version": "3.0.0",
      "connector": "xtdb-cdc",
      "name": "xtdb",
      "ts_ms": 1705312800000,
      "db": "xtdb",
      "table": "products",
      "tx_id": 2001
    },
    "op": "c",
    "ts_ms": 1705312800000
  }
}
EOF

# Order with complex nested structure
cat > cdc-output-example/orders/001_c_order-789.json << 'EOF'
{
  "schema": {
    "type": "struct",
    "name": "xtdb.kafka.cdc.Envelope"
  },
  "payload": {
    "before": null,
    "after": {
      "_id": "order-789",
      "_valid_from": "2024-01-15T11:45:00Z",
      "_valid_to": "9999-12-31T23:59:59.999999Z",
      "_system_from": "2024-01-15T11:45:00Z",
      "_system_to": "9999-12-31T23:59:59.999999Z",
      "order_number": "ORD-2024-000789",
      "customer_id": "user-123",
      "order_date": "2024-01-15T11:45:00Z",
      "status": "pending",
      "items": [
        {
          "product_id": "prod-456",
          "product_name": "Widget Pro",
          "quantity": 2,
          "unit_price": 29.99,
          "subtotal": 59.98,
          "discount_percent": 10,
          "final_price": 53.98
        },
        {
          "product_id": "prod-789",
          "product_name": "Gadget Plus",
          "quantity": 1,
          "unit_price": 49.99,
          "subtotal": 49.99,
          "discount_percent": 0,
          "final_price": 49.99
        }
      ],
      "pricing": {
        "subtotal": 103.97,
        "discount": 5.99,
        "tax_rate": 0.0875,
        "tax_amount": 9.10,
        "shipping": 5.00,
        "total": 118.07,
        "currency": "USD"
      },
      "shipping_address": {
        "name": "Alice Johnson-Smith",
        "street": "123 Main Street",
        "city": "Springfield",
        "state": "CA",
        "zip": "90210",
        "country": "USA",
        "instructions": "Leave at door"
      },
      "billing_address": {
        "same_as_shipping": true
      },
      "payment": {
        "method": "credit_card",
        "last_four": "4242",
        "processor": "stripe",
        "transaction_id": "ch_3MQpG2H7g8J9K0L1"
      },
      "metadata": {
        "source": "web",
        "ip_address": "192.168.1.100",
        "session_id": "sess-abc-123-def",
        "utm_source": "email",
        "utm_campaign": "winter_sale"
      }
    },
    "source": {
      "version": "3.0.0",
      "connector": "xtdb-cdc",
      "name": "xtdb",
      "ts_ms": 1705320300000,
      "db": "xtdb",
      "table": "orders",
      "tx_id": 3001
    },
    "op": "c",
    "ts_ms": 1705320300000
  }
}
EOF

# Create a summary file
cat > cdc-output-example/README.md << 'EOF'
# XTDB Kafka CDC Output Examples

This directory contains example CDC (Change Data Capture) events in Debezium-compatible format.

## Directory Structure

- `users/` - User lifecycle events (CREATE, UPDATE, DELETE)
- `products/` - Product catalog changes
- `orders/` - Order transactions with complex nested data

## Event Format

Each JSON file represents a single CDC event with:

1. **Schema**: Describes the structure (Avro-compatible)
2. **Payload**: The actual event data containing:
   - `before`: State before the change (null for CREATE)
   - `after`: State after the change (null for DELETE)
   - `source`: Metadata about the source system
   - `op`: Operation type (c=create, u=update, d=delete, r=read)
   - `ts_ms`: Timestamp in milliseconds

## Operation Types

- `c` - CREATE: New record inserted
- `u` - UPDATE: Existing record modified
- `d` - DELETE: Record removed
- `r` - READ: Snapshot read (used during initial sync)

## XTDB Temporal Columns

Each XTDB record includes temporal metadata:
- `_valid_from`: Start of valid time (business time) for this version
- `_valid_to`: End of valid time (business time) for this version  
- `_system_from`: When this version was recorded in the system
- `_system_to`: When this version was superseded in the system

For current records, `_valid_to` and `_system_to` are set to the maximum timestamp.
For updated records, the old version gets its end times set to the update timestamp.

## XTDB Type Handling

The CDC events preserve XTDB's Arrow types:
- Temporal types (Instant, LocalDate, Duration) as ISO-8601 strings
- BigDecimal as string to preserve precision
- UUID as string
- Binary data as Base64
- Complex nested structures as JSON objects/arrays
- Temporal columns (`_valid_from`, `_valid_to`, `_system_from`, `_system_to`) as ISO-8601 timestamps

## Integration with Kafka

These events would be sent to Kafka topics named:
- `xtdb.cdc.users`
- `xtdb.cdc.products`
- `xtdb.cdc.orders`

Consumers can use Kafka Connect, Kafka Streams, or any Debezium-compatible tool to process these events.
EOF

echo "✅ CDC example files created in: cdc-output-example/"
echo ""
echo "Directory contents:"
ls -la cdc-output-example/
echo ""
echo "Tables with events:"
for dir in cdc-output-example/*/; do
    if [ -d "$dir" ]; then
        count=$(ls -1 "$dir"*.json 2>/dev/null | wc -l)
        if [ $count -gt 0 ]; then
            echo "  - $(basename $dir): $count events"
        fi
    fi
done
echo ""
echo "You can view the JSON files to see the Debezium-compatible CDC format."
echo "Example: cat cdc-output-example/users/001_c_user-123.json | jq ."