# Sample CDC Topic Events

This document shows examples of what CDC events look like in Kafka topics for various XTDB operations.

## Topic Structure

CDC events are published to topics named: `{topic-prefix}{table-name}`

For example:
- `xtdb.cdc.users` - CDC events for the `users` table
- `xtdb.cdc.orders` - CDC events for the `orders` table
- `xtdb.cdc.products` - CDC events for the `products` table

## Sample CDC Events

### 1. CREATE Operation (Insert New User)

**XTDB Transaction:**
```clojure
(xt/submit-tx node
  [[:put-docs :users {:xt/id "user-123"
                      :name "John Doe"
                      :email "john@example.com"
                      :created-at #inst "2023-12-25T10:15:30.123Z"
                      :age 30
                      :is-active true
                      :balance 999.95M
                      :uuid #uuid "550e8400-e29b-41d4-a716-446655440000"}]])
```

**CDC Event in `xtdb.cdc.users` topic:**
```json
{
  "before": null,
  "after": "{\"_id\":\"user-123\",\"name\":\"John Doe\",\"email\":\"john@example.com\",\"created_at\":\"2023-12-25T10:15:30.123Z\",\"age\":30,\"is_active\":true,\"balance\":\"999.95\",\"uuid\":\"550e8400-e29b-41d4-a716-446655440000\"}",
  "source": {
    "version": "3.0.0",
    "connector": "xtdb-cdc",
    "name": "xtdb-node-1",
    "ts_ms": 1703505330123,
    "snapshot": null,
    "db": "mydb",
    "table": "users",
    "tx_id": 12345,
    "system_time": 1703505330123
  },
  "op": "c",
  "ts_ms": 1703505330123,
  "transaction": {
    "id": "12345",
    "total_order": 12345,
    "data_collection_order": 0
  }
}
```

### 2. UPDATE Operation (Patch User)

**XTDB Transaction:**
```clojure
(xt/submit-tx node
  [[:patch-docs :users {:xt/id "user-123"
                        :email "john.doe@example.com"
                        :age 31
                        :last-updated #inst "2023-12-26T14:30:15.456Z"}]])
```

**CDC Event in `xtdb.cdc.users` topic:**
```json
{
  "before": "{\"_id\":\"user-123\",\"name\":\"John Doe\",\"email\":\"john@example.com\",\"created_at\":\"2023-12-25T10:15:30.123Z\",\"age\":30,\"is_active\":true,\"balance\":\"999.95\",\"uuid\":\"550e8400-e29b-41d4-a716-446655440000\"}",
  "after": "{\"_id\":\"user-123\",\"name\":\"John Doe\",\"email\":\"john.doe@example.com\",\"created_at\":\"2023-12-25T10:15:30.123Z\",\"age\":31,\"is_active\":true,\"balance\":\"999.95\",\"uuid\":\"550e8400-e29b-41d4-a716-446655440000\",\"last_updated\":\"2023-12-26T14:30:15.456Z\"}",
  "source": {
    "version": "3.0.0",
    "connector": "xtdb-cdc",
    "name": "xtdb-node-1",
    "ts_ms": 1703595015456,
    "snapshot": null,
    "db": "mydb",
    "table": "users",
    "tx_id": 12346,
    "system_time": 1703595015456
  },
  "op": "u",
  "ts_ms": 1703595015456,
  "transaction": {
    "id": "12346",
    "total_order": 12346,
    "data_collection_order": 0
  }
}
```

### 3. DELETE Operation

**XTDB Transaction:**
```clojure
(xt/submit-tx node
  [[:delete-docs :users "user-123"]])
```

**CDC Event in `xtdb.cdc.users` topic:**
```json
{
  "before": "{\"_id\":\"user-123\",\"name\":\"John Doe\",\"email\":\"john.doe@example.com\",\"created_at\":\"2023-12-25T10:15:30.123Z\",\"age\":31,\"is_active\":true,\"balance\":\"999.95\",\"uuid\":\"550e8400-e29b-41d4-a716-446655440000\",\"last_updated\":\"2023-12-26T14:30:15.456Z\"}",
  "after": null,
  "source": {
    "version": "3.0.0",
    "connector": "xtdb-cdc",
    "name": "xtdb-node-1",
    "ts_ms": 1703681415789,
    "snapshot": null,
    "db": "mydb",
    "table": "users",
    "tx_id": 12347,
    "system_time": 1703681415789
  },
  "op": "d",
  "ts_ms": 1703681415789,
  "transaction": {
    "id": "12347",
    "total_order": 12347,
    "data_collection_order": 0
  }
}
```

### 4. Complex Data Types Example

**XTDB Transaction with Complex Types:**
```clojure
(xt/submit-tx node
  [[:put-docs :orders {:xt/id "order-456"
                       :customer-id "user-123"
                       :order-date #inst "2023-12-25T15:30:00.000Z"
                       :delivery-period #xtdb/period "P3D"
                       :processing-duration #xtdb/duration "PT2H30M"
                       :total-amount 1234.56M
                       :metadata {:tags ["urgent" "priority"]
                                  :notes "Rush delivery"
                                  :coordinates [40.7128 -74.0060]
                                  :binary-data (byte-array [0x01 0x02 0x03 0xFF])}
                       :valid-interval {:months 6 :days 15 :nanos 0}
                       :items [{:product-id "prod-1" :quantity 2 :price 25.50M}
                               {:product-id "prod-2" :quantity 1 :price 199.99M}]}]])
```

**CDC Event in `xtdb.cdc.orders` topic:**
```json
{
  "before": null,
  "after": "{\"_id\":\"order-456\",\"customer_id\":\"user-123\",\"order_date\":\"2023-12-25T15:30:00Z\",\"delivery_period\":\"P3D\",\"processing_duration\":\"PT2H30M\",\"total_amount\":\"1234.56\",\"metadata\":{\"tags\":[\"urgent\",\"priority\"],\"notes\":\"Rush delivery\",\"coordinates\":[40.7128,-74.0060],\"binary_data\":\"AQID/w==\"},\"valid_interval\":{\"months\":6,\"days\":15,\"nanos\":0},\"items\":[{\"product_id\":\"prod-1\",\"quantity\":2,\"price\":\"25.50\"},{\"product_id\":\"prod-2\",\"quantity\":1,\"price\":\"199.99\"}]}",
  "source": {
    "version": "3.0.0",
    "connector": "xtdb-cdc",
    "name": "xtdb-node-1",
    "ts_ms": 1703523000000,
    "snapshot": null,
    "db": "mydb",
    "table": "orders",
    "tx_id": 12348,
    "system_time": 1703523000000
  },
  "op": "c",
  "ts_ms": 1703523000000,
  "transaction": {
    "id": "12348",
    "total_order": 12348,
    "data_collection_order": 0
  }
}
```

### 5. SQL Operation Example

**XTDB SQL Transaction:**
```sql
INSERT INTO products (_id, name, category, price, created_at, tags)
VALUES ('prod-789', 'Premium Widget', 'electronics', 299.99, '2023-12-25T16:45:00Z', ['featured', 'bestseller'])
```

**CDC Event in `xtdb.cdc.products` topic:**
```json
{
  "before": null,
  "after": "{\"_id\":\"prod-789\",\"name\":\"Premium Widget\",\"category\":\"electronics\",\"price\":299.99,\"created_at\":\"2023-12-25T16:45:00Z\",\"tags\":[\"featured\",\"bestseller\"]}",
  "source": {
    "version": "3.0.0",
    "connector": "xtdb-cdc",
    "name": "xtdb-node-1",
    "ts_ms": 1703527500000,
    "snapshot": null,
    "db": "mydb",
    "table": "products",
    "tx_id": 12349,
    "system_time": 1703527500000
  },
  "op": "c",
  "ts_ms": 1703527500000,
  "transaction": {
    "id": "12349",
    "total_order": 12349,
    "data_collection_order": 0
  }
}
```

### 6. Multi-Operation Transaction

**XTDB Transaction with Multiple Operations:**
```clojure
(xt/submit-tx node
  [[:put-docs :users {:xt/id "user-999" :name "Jane Smith" :email "jane@example.com"}]
   [:patch-docs :users {:xt/id "user-123" :last-login #inst "2023-12-25T18:00:00Z"}]
   [:delete-docs :orders "order-old-1"]])
```

This generates **three separate CDC events** with the same transaction ID but different `data_collection_order`:

**Event 1 - `xtdb.cdc.users` topic:**
```json
{
  "before": null,
  "after": "{\"_id\":\"user-999\",\"name\":\"Jane Smith\",\"email\":\"jane@example.com\"}",
  "source": {
    "version": "3.0.0",
    "connector": "xtdb-cdc",
    "name": "xtdb-node-1",
    "ts_ms": 1703532000000,
    "snapshot": null,
    "db": "mydb",
    "table": "users",
    "tx_id": 12350,
    "system_time": 1703532000000
  },
  "op": "c",
  "ts_ms": 1703532000000,
  "transaction": {
    "id": "12350",
    "total_order": 12350,
    "data_collection_order": 0
  }
}
```

**Event 2 - `xtdb.cdc.users` topic:**
```json
{
  "before": "{\"_id\":\"user-123\",\"name\":\"John Doe\",\"email\":\"john.doe@example.com\",\"age\":31,\"is_active\":true,\"balance\":\"999.95\",\"last_updated\":\"2023-12-26T14:30:15.456Z\"}",
  "after": "{\"_id\":\"user-123\",\"name\":\"John Doe\",\"email\":\"john.doe@example.com\",\"age\":31,\"is_active\":true,\"balance\":\"999.95\",\"last_updated\":\"2023-12-26T14:30:15.456Z\",\"last_login\":\"2023-12-25T18:00:00Z\"}",
  "source": {
    "version": "3.0.0",
    "connector": "xtdb-cdc",
    "name": "xtdb-node-1",
    "ts_ms": 1703532000000,
    "snapshot": null,
    "db": "mydb",
    "table": "users",
    "tx_id": 12350,
    "system_time": 1703532000000
  },
  "op": "u",
  "ts_ms": 1703532000000,
  "transaction": {
    "id": "12350",
    "total_order": 12350,
    "data_collection_order": 1
  }
}
```

**Event 3 - `xtdb.cdc.orders` topic:**
```json
{
  "before": "{\"_id\":\"order-old-1\",\"customer_id\":\"user-456\",\"total\":150.00,\"status\":\"completed\"}",
  "after": null,
  "source": {
    "version": "3.0.0",
    "connector": "xtdb-cdc",
    "name": "xtdb-node-1",
    "ts_ms": 1703532000000,
    "snapshot": null,
    "db": "mydb",
    "table": "orders",
    "tx_id": 12350,
    "system_time": 1703532000000
  },
  "op": "d",
  "ts_ms": 1703532000000,
  "transaction": {
    "id": "12350",
    "total_order": 12350,
    "data_collection_order": 2
  }
}
```

## Key Characteristics

### **Topic Partitioning**
- Events are partitioned by record ID (the key of the Kafka message)
- Ensures ordering within a single entity across all operations
- Record ID is extracted from `_id` or `xt/id` field

### **Message Keys**
- Kafka message key is the record ID (e.g., "user-123", "order-456")
- Enables proper partitioning and ordering guarantees
- Allows consumers to track changes to specific entities

### **Transaction Ordering**
- `tx_id` provides global ordering across all tables
- `data_collection_order` provides ordering within a transaction
- `total_order` is the absolute position in the transaction log

### **Data Types in JSON**
- **Timestamps**: ISO-8601 strings
- **Decimals**: String representation for precision
- **Binary**: Base64 encoded strings
- **UUIDs**: Standard string format
- **Durations/Periods**: ISO-8601 format
- **Intervals**: JSON objects with semantic fields
- **Arrays**: JSON arrays
- **Nested objects**: Full JSON structure

### **Debezium Compatibility**
- Standard Debezium envelope format
- Compatible with Debezium sink connectors
- Works with Kafka Connect ecosystem
- Suitable for Debezium Server deployment

This format ensures that CDC consumers can:
1. Reconstruct complete entity state from before/after values
2. Maintain transaction boundaries across multiple topics
3. Handle all XTDB data types correctly
4. Integrate with existing Debezium-based pipelines