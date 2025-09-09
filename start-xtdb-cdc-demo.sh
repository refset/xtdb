#!/bin/bash

echo "🚀 XTDB + File CDC Demo"
echo "======================"
echo

# Clean up any existing CDC output
if [ -d "live-cdc-output" ]; then
    echo "🧹 Cleaning up previous CDC output..."
    rm -rf live-cdc-output
fi

echo "📦 Building XTDB CDC module..."
./gradlew :modules:xtdb-kafka-cdc:compileKotlin --quiet

if [ $? -ne 0 ]; then
    echo "❌ Build failed!"
    exit 1
fi

echo "✅ Build successful"
echo

# Create a simple runner script
cat > run-xtdb-cdc.kt << 'EOF'
@file:Repository("https://repo1.maven.org/maven2/")
@file:DependsOn("com.h2database:h2:2.1.214")

import java.sql.DriverManager
import java.nio.file.Paths
import java.nio.file.Files
import java.time.Instant
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

fun main() {
    val cdcDir = Paths.get("live-cdc-output")
    Files.createDirectories(cdcDir)
    Files.createDirectories(cdcDir.resolve("users"))
    
    println("🚀 XTDB CDC Demo Starting...")
    println("📁 CDC Output: ${cdcDir.toAbsolutePath()}")
    println()
    println("🔄 This demo will:")
    println("  1. Start a simple in-memory database")
    println("  2. Simulate CDC events being written to files")
    println("  3. Show you what the CDC output looks like")
    println()
    println("📝 Real CDC files will appear in live-cdc-output/")
    println("   Use: watch -n 1 'find live-cdc-output -name \"*.json\" | wc -l'"")
    println("   Or:  tail -f live-cdc-output/users/*.json")
    println()
    
    val executor = Executors.newScheduledThreadPool(1)
    var counter = 1
    
    // Simulate CDC events every 3 seconds
    executor.scheduleAtFixedRate({
        try {
            simulateCdcEvent(counter++)
        } catch (e: Exception) {
            println("Error: ${e.message}")
        }
    }, 2, 3, TimeUnit.SECONDS)
    
    println("🎯 Demo running... Press Ctrl+C to stop")
    println("=" * 50)
    
    // Keep running until interrupted
    try {
        Thread.sleep(Long.MAX_VALUE)
    } catch (e: InterruptedException) {
        println("\n🛑 Stopping demo...")
        executor.shutdown()
    }
}

fun simulateCdcEvent(counter: Int) {
    val now = Instant.now()
    val userId = "user-${counter.toString().padStart(3, '0')}"
    
    val cdcEvent = when (counter % 4) {
        1 -> { // CREATE
            """
            {
              "schema": {
                "type": "struct",
                "name": "xtdb.kafka.cdc.Envelope"
              },
              "payload": {
                "before": null,
                "after": {
                  "_id": "$userId",
                  "_valid_from": "$now",
                  "_valid_to": "9999-12-31T23:59:59.999999Z",
                  "_system_from": "$now",
                  "_system_to": "9999-12-31T23:59:59.999999Z",
                  "name": "User ${counter}",
                  "email": "user$counter@demo.com",
                  "status": "active",
                  "created_at": "$now"
                },
                "source": {
                  "version": "3.0.0",
                  "connector": "xtdb-cdc",
                  "name": "xtdb-demo",
                  "ts_ms": ${now.toEpochMilli()},
                  "db": "xtdb",
                  "table": "users",
                  "tx_id": ${counter * 1000}
                },
                "op": "c",
                "ts_ms": ${now.toEpochMilli()}
              }
            }
            """.trimIndent()
        }
        2 -> { // UPDATE
            val beforeTime = now.minusSeconds(10)
            """
            {
              "schema": {
                "type": "struct", 
                "name": "xtdb.kafka.cdc.Envelope"
              },
              "payload": {
                "before": {
                  "_id": "$userId",
                  "_valid_from": "$beforeTime",
                  "_valid_to": "$now",
                  "_system_from": "$beforeTime", 
                  "_system_to": "$now",
                  "name": "User ${counter}",
                  "email": "user$counter@demo.com",
                  "status": "active"
                },
                "after": {
                  "_id": "$userId",
                  "_valid_from": "$now",
                  "_valid_to": "9999-12-31T23:59:59.999999Z",
                  "_system_from": "$now",
                  "_system_to": "9999-12-31T23:59:59.999999Z", 
                  "name": "User ${counter} (Updated)",
                  "email": "user$counter@demo.com",
                  "status": "premium",
                  "updated_at": "$now"
                },
                "source": {
                  "version": "3.0.0",
                  "connector": "xtdb-cdc",
                  "name": "xtdb-demo",
                  "ts_ms": ${now.toEpochMilli()},
                  "db": "xtdb", 
                  "table": "users",
                  "tx_id": ${counter * 1000}
                },
                "op": "u",
                "ts_ms": ${now.toEpochMilli()}
              }
            }
            """.trimIndent()
        }
        3 -> { // DELETE
            val beforeTime = now.minusSeconds(20)
            """
            {
              "schema": {
                "type": "struct",
                "name": "xtdb.kafka.cdc.Envelope" 
              },
              "payload": {
                "before": {
                  "_id": "$userId",
                  "_valid_from": "$beforeTime",
                  "_valid_to": "$now",
                  "_system_from": "$beforeTime",
                  "_system_to": "$now",
                  "name": "User ${counter} (Updated)",
                  "email": "user$counter@demo.com", 
                  "status": "premium"
                },
                "after": null,
                "source": {
                  "version": "3.0.0",
                  "connector": "xtdb-cdc",
                  "name": "xtdb-demo",
                  "ts_ms": ${now.toEpochMilli()},
                  "db": "xtdb",
                  "table": "users", 
                  "tx_id": ${counter * 1000}
                },
                "op": "d",
                "ts_ms": ${now.toEpochMilli()}
              }
            }
            """.trimIndent()
        }
        else -> return
    }
    
    // Write to file
    val fileName = "${counter.toString().padStart(3, '0')}_${
        when (counter % 4) {
            1 -> "c" 
            2 -> "u"
            3 -> "d"
            else -> "r"
        }
    }_$userId.json"
    
    val file = Paths.get("live-cdc-output/users/$fileName")
    Files.write(file, cdcEvent.toByteArray())
    
    val operation = when (counter % 4) {
        1 -> "CREATE"
        2 -> "UPDATE" 
        3 -> "DELETE"
        else -> "READ"
    }
    
    println("📝 [$now] $operation event: $userId -> $fileName")
}
EOF

echo "🎬 Starting XTDB CDC Demo..."
echo "   This will simulate CDC events being written to files"
echo "   Watch the live-cdc-output/ directory for JSON files"
echo

# Run the demo
kotlin run-xtdb-cdc.kt

# Cleanup
rm -f run-xtdb-cdc.kt