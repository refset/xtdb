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
