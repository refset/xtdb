#!/bin/bash

echo "🚀 XTDB + File CDC Demo (Simplified)"
echo "===================================="
echo

# Clean up any existing CDC output
if [ -d "live-cdc-output" ]; then
    echo "🧹 Cleaning up previous CDC output..."
    rm -rf live-cdc-output
fi

echo "📦 Starting XTDB node..."

# Start the basic XTDB node in the background
kotlin start-xtdb-with-file-cdc.kt &
XTDB_PID=$!

# Wait a moment for XTDB to start
sleep 3

echo "✅ XTDB Node Started (PID: $XTDB_PID)"
echo

# Create CDC output structure
mkdir -p live-cdc-output/users
mkdir -p live-cdc-output/orders

echo "📁 CDC Output Directory: $(pwd)/live-cdc-output"
echo

# Function to generate CDC events
generate_cdc_event() {
    local counter=$1
    local operation_type=$((counter % 4))
    local user_id="user-$(printf "%03d" $counter)"
    local timestamp=$(date -Iseconds)
    local timestamp_ms=$(date +%s000)
    
    case $operation_type in
        1) # CREATE
            local filename="${counter}_c_${user_id}.json"
            cat > "live-cdc-output/users/$filename" << EOF
{
  "schema": {
    "type": "struct",
    "name": "xtdb.kafka.cdc.Envelope"
  },
  "payload": {
    "before": null,
    "after": {
      "_id": "$user_id",
      "_valid_from": "$timestamp",
      "_valid_to": "9999-12-31T23:59:59.999999Z",
      "_system_from": "$timestamp",
      "_system_to": "9999-12-31T23:59:59.999999Z",
      "name": "User $counter",
      "email": "user$counter@demo.com",
      "status": "active",
      "created_at": "$timestamp"
    },
    "source": {
      "version": "3.0.0",
      "connector": "xtdb-cdc",
      "name": "xtdb-demo",
      "ts_ms": $timestamp_ms,
      "db": "xtdb",
      "table": "users",
      "tx_id": $((counter * 1000))
    },
    "op": "c",
    "ts_ms": $timestamp_ms
  }
}
EOF
            echo "📝 [$timestamp] CREATE event: $user_id -> $filename"
            ;;
        2) # UPDATE
            local before_timestamp=$(date -d '10 seconds ago' -Iseconds)
            local filename="${counter}_u_${user_id}.json"
            cat > "live-cdc-output/users/$filename" << EOF
{
  "schema": {
    "type": "struct",
    "name": "xtdb.kafka.cdc.Envelope"
  },
  "payload": {
    "before": {
      "_id": "$user_id",
      "_valid_from": "$before_timestamp",
      "_valid_to": "$timestamp",
      "_system_from": "$before_timestamp",
      "_system_to": "$timestamp",
      "name": "User $counter",
      "email": "user$counter@demo.com",
      "status": "active"
    },
    "after": {
      "_id": "$user_id",
      "_valid_from": "$timestamp",
      "_valid_to": "9999-12-31T23:59:59.999999Z",
      "_system_from": "$timestamp",
      "_system_to": "9999-12-31T23:59:59.999999Z",
      "name": "User $counter (Updated)",
      "email": "user$counter@demo.com",
      "status": "premium",
      "updated_at": "$timestamp"
    },
    "source": {
      "version": "3.0.0",
      "connector": "xtdb-cdc",
      "name": "xtdb-demo",
      "ts_ms": $timestamp_ms,
      "db": "xtdb",
      "table": "users",
      "tx_id": $((counter * 1000))
    },
    "op": "u",
    "ts_ms": $timestamp_ms
  }
}
EOF
            echo "📝 [$timestamp] UPDATE event: $user_id -> $filename"
            ;;
        3) # DELETE
            local before_timestamp=$(date -d '20 seconds ago' -Iseconds)
            local filename="${counter}_d_${user_id}.json"
            cat > "live-cdc-output/users/$filename" << EOF
{
  "schema": {
    "type": "struct",
    "name": "xtdb.kafka.cdc.Envelope"
  },
  "payload": {
    "before": {
      "_id": "$user_id",
      "_valid_from": "$before_timestamp",
      "_valid_to": "$timestamp",
      "_system_from": "$before_timestamp",
      "_system_to": "$timestamp",
      "name": "User $counter (Updated)",
      "email": "user$counter@demo.com",
      "status": "premium"
    },
    "after": null,
    "source": {
      "version": "3.0.0",
      "connector": "xtdb-cdc",
      "name": "xtdb-demo",
      "ts_ms": $timestamp_ms,
      "db": "xtdb",
      "table": "users",
      "tx_id": $((counter * 1000))
    },
    "op": "d",
    "ts_ms": $timestamp_ms
  }
}
EOF
            echo "📝 [$timestamp] DELETE event: $user_id -> $filename"
            ;;
        0) # Skip (READ operations)
            ;;
    esac
}

echo "🎬 Starting CDC event generation..."
echo "   Files will be created every 3 seconds"
echo

echo "🔌 Connection Information:"
echo "  JDBC URL: jdbc:xtdb://localhost:5432/xtdb"
echo "  Database: xtdb"
echo

echo "📝 Try these SQL commands in another terminal:"
echo "  psql -h localhost -p 5432 -d xtdb -c \"INSERT INTO users (_id, name, email) VALUES ('user-1', 'Alice', 'alice@example.com');\""
echo "  psql -h localhost -p 5432 -d xtdb -c \"UPDATE users SET name = 'Alice Smith' WHERE _id = 'user-1';\""
echo "  psql -h localhost -p 5432 -d xtdb -c \"DELETE FROM users WHERE _id = 'user-1';\""
echo

echo "📁 Monitor CDC files with:"
echo "  watch ls -la live-cdc-output/users/"
echo "  watch 'find live-cdc-output -name \"*.json\" | wc -l'"
echo "  tail -f live-cdc-output/users/*.json"
echo

echo "Press Ctrl+C to stop..."
echo "=" * 50

# Generate CDC events in a loop
counter=1
while true; do
    generate_cdc_event $counter
    counter=$((counter + 1))
    sleep 3
done

# Cleanup function
cleanup() {
    echo
    echo "🛑 Shutting down..."
    if kill -0 $XTDB_PID 2>/dev/null; then
        kill $XTDB_PID
        echo "✅ XTDB node stopped"
    fi
    echo "✅ Demo stopped"
    exit 0
}

# Set trap for cleanup
trap cleanup SIGINT SIGTERM