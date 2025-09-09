# XTDB File-Based CDC Demo

This demo shows you what XTDB CDC events look like when written to files instead of a Kafka topic.

## Quick Start

Run the demo:
```bash
./simple-cdc-demo.sh
```

This will:
- Start an XTDB node on port 5432 (if kotlin is available)
- Generate CDC events every 3 seconds to `live-cdc-output/users/` 
- Show CREATE, UPDATE, and DELETE events with XTDB temporal columns

## Monitoring CDC Files

In another terminal, you can monitor the CDC files being created:

```bash
# Watch files being created
watch ls -la live-cdc-output/users/

# Count total CDC files
watch 'find live-cdc-output -name "*.json" | wc -l'

# View latest CDC events  
tail -f live-cdc-output/users/*.json

# View a specific CDC file
cat live-cdc-output/users/1_c_user-001.json
```

## Testing with Real XTDB (if kotlin is available)

Connect to XTDB via JDBC and run SQL commands:

```bash
# Connect to XTDB
psql -h localhost -p 5432 -d xtdb

# Run some SQL commands
INSERT INTO users (_id, name, email) VALUES ('user-1', 'Alice', 'alice@example.com');
UPDATE users SET name = 'Alice Smith' WHERE _id = 'user-1';
DELETE FROM users WHERE _id = 'user-1';
```

*Note: Real XTDB integration requires completing the CDC processor integration.*

## Understanding the CDC Events

Each CDC file contains:

### Schema
- Defines the Debezium envelope structure
- Compatible with standard CDC consumers

### Payload
- **before**: Previous state of the record (null for CREATE, populated for UPDATE/DELETE)
- **after**: New state of the record (populated for CREATE/UPDATE, null for DELETE) 
- **source**: Metadata about the change (database, table, transaction ID, etc.)
- **op**: Operation type (`c` = CREATE, `u` = UPDATE, `d` = DELETE)

### XTDB Temporal Columns

Each record includes XTDB's bitemporal columns:

- **`_valid_from`**: When the data became valid (business time)
- **`_valid_to`**: When the data stopped being valid (business time)
- **`_system_from`**: When the data was recorded in XTDB (system time)  
- **`_system_to`**: When the data was superseded in XTDB (system time)

For current/active records, `_valid_to` and `_system_to` are set to `9999-12-31T23:59:59.999999Z`.

### Example Files

- `001_c_user-001.json` - CREATE operation for user-001
- `002_u_user-002.json` - UPDATE operation for user-002  
- `003_d_user-003.json` - DELETE operation for user-003

## File Structure

```
live-cdc-output/
├── users/
│   ├── 001_c_user-001.json
│   ├── 002_u_user-002.json
│   ├── 003_d_user-003.json
│   └── ...
└── orders/ (for future table demos)
```

## Stopping the Demo

Press `Ctrl+C` to stop the demo and cleanup processes.