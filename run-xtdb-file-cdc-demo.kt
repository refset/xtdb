#!/usr/bin/env kotlin

@file:Repository("https://repo1.maven.org/maven2/")
@file:DependsOn("org.xtdb:xtdb-core:3.0.0")
@file:DependsOn("org.xtdb:xtdb-api:3.0.0")

import xtdb.api.Xtdb
import xtdb.api.storage.Storage
import xtdb.api.log.Log
import java.nio.file.Paths
import java.nio.file.Files
import java.time.Instant

/**
 * Demo: XTDB node with file-based CDC simulation
 * 
 * This creates an XTDB node and simulates CDC events being written to files
 * so you can see what CDC output would look like.
 */
fun main() {
    val cdcOutputDir = Paths.get("live-cdc-output")
    
    // Clean up any existing CDC output
    if (Files.exists(cdcOutputDir)) {
        println("🧹 Cleaning up previous CDC output...")
        cdcOutputDir.toFile().deleteRecursively()
    }
    
    Files.createDirectories(cdcOutputDir)
    Files.createDirectories(cdcOutputDir.resolve("users"))
    Files.createDirectories(cdcOutputDir.resolve("orders"))
    
    println("🚀 Starting XTDB with File-Based CDC Demo")
    println("=" * 50)
    println("CDC Output Directory: ${cdcOutputDir.toAbsolutePath()}")
    println("JDBC Connection: jdbc:xtdb://localhost:5432/xtdb")
    println("=" * 50)
    println()
    
    // Start XTDB with in-memory storage
    val xtdb = Xtdb.Config()
        .server {
            port = 5432
            readOnlyPort = 3000
        }
        .logCluster("memory-log", Log.InMemory.Factory())
        .database("xtdb", xtdb.database.Database.Config().apply {
            logCluster = "memory-log"
            storage = Storage.InMemory.Factory()
        })
        .open()
    
    println("✅ XTDB Node Started Successfully!")
    println()
    println("🔌 Connection Information:")
    println("  JDBC URL: jdbc:xtdb://localhost:5432/xtdb")
    println("  Read-Only Port: 3000")
    println("  Database: xtdb")
    println()
    println("📝 Try these SQL commands in another terminal:")
    println("  psql -h localhost -p 5432 -d xtdb -c \"INSERT INTO users (_id, name, email) VALUES ('user-1', 'Alice', 'alice@example.com');\"")
    println("  psql -h localhost -p 5432 -d xtdb -c \"UPDATE users SET name = 'Alice Smith' WHERE _id = 'user-1';\"")
    println("  psql -h localhost -p 5432 -d xtdb -c \"DELETE FROM users WHERE _id = 'user-1';\"")
    println()
    println("📁 CDC files will appear in: $cdcOutputDir")
    println("   Use 'watch ls -la $cdcOutputDir/*/' to monitor in real-time")
    println("   Or: 'watch find $cdcOutputDir -name '*.json' | wc -l' to count files")
    println()
    
    // Start CDC simulation in a separate thread
    val cdcThread = Thread {
        var counter = 1
        while (!Thread.currentThread().isInterrupted) {
            try {
                Thread.sleep(5000) // Wait 5 seconds between demo events
                simulateCdcEvent(cdcOutputDir, counter++)
            } catch (e: InterruptedException) {
                break
            } catch (e: Exception) {
                println("Error in CDC simulation: ${e.message}")
            }
        }
    }
    
    cdcThread.start()
    println("🎬 CDC Demo Started! Files will be written every 5 seconds...")
    println("📊 Real database changes via JDBC will also appear in CDC files (when integrated)")
    println()
    println("Press Ctrl+C to stop...")
    
    // Shutdown hook
    Runtime.getRuntime().addShutdownHook(Thread {
        println("\n🛑 Shutting down...")
        try {
            cdcThread.interrupt()
            xtdb.close()
            println("✅ Shutdown complete")
        } catch (e: Exception) {
            println("Error during shutdown: ${e.message}")
        }
    })
    
    // Keep running until interrupted
    try {
        Thread.currentThread().join()
    } catch (e: InterruptedException) {
        // Will be handled by shutdown hook
    }
}

private fun simulateCdcEvent(cdcOutputDir: Paths, counter: Int) {
    val now = Instant.now()
    val userId = "user-${counter.toString().padStart(3, '0')}"
    
    val operation = when (counter % 4) {
        1 -> "CREATE"
        2 -> "UPDATE"
        3 -> "DELETE"
        else -> "READ"
    }
    
    if (counter % 4 == 0) return // Skip READ operations for demo
    
    // Write to file as JSON string
    val fileName = "${counter.toString().padStart(3, '0')}_${
        when (counter % 4) {
            1 -> "c"
            2 -> "u" 
            3 -> "d"
            else -> "r"
        }
    }_$userId.json"
    
    val file = cdcOutputDir.resolve("users").resolve(fileName)
    
    // Convert to JSON manually for this demo (to avoid Jackson dependency)
    val jsonString = when (counter % 4) {
        1 -> // CREATE
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
                  "name": "User $counter",
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
                  "name": "User $counter",
                  "email": "user$counter@demo.com",
                  "status": "active"
                },
                "after": {
                  "_id": "$userId",
                  "_valid_from": "$now",
                  "_valid_to": "9999-12-31T23:59:59.999999Z",
                  "_system_from": "$now",
                  "_system_to": "9999-12-31T23:59:59.999999Z",
                  "name": "User $counter (Updated)",
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
                  "name": "User $counter (Updated)",
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
        else -> ""
    }
    
    Files.write(file, jsonString.toByteArray())
    
    println("📝 [$now] $operation event: $userId -> $fileName")
}

private operator fun String.times(count: Int): String = repeat(count)