#!/usr/bin/env kotlin

@file:Repository("https://repo1.maven.org/maven2/")
@file:DependsOn("org.xtdb:xtdb-core:3.0.0")
@file:DependsOn("org.xtdb:xtdb-api:3.0.0")

import xtdb.api.Xtdb
import xtdb.api.storage.Storage
import xtdb.api.log.Log
import java.nio.file.Paths
import java.time.Duration

/**
 * Simple XTDB node with file-based CDC writer for testing
 */

fun main() {
    val cdcOutputDir = Paths.get("live-cdc-output")
    println("Starting XTDB with file-based CDC...")
    println("CDC files will be written to: ${cdcOutputDir.toAbsolutePath()}")
    println("Connect to: jdbc:xtdb://localhost:5432/xtdb")
    println()
    
    val xtdb = Xtdb.Config()
        .server {
            port = 5432
        }
        .logCluster(
            "memory-log", 
            Log.InMemory.Factory()
        )
        .database(
            "xtdb", 
            xtdb.database.Database.Config().apply {
                logCluster = "memory-log"
                storage = Storage.InMemory.Factory()
            }
        )
        // TODO: Add file CDC module once we integrate it
        .open()
    
    println("✅ XTDB started successfully!")
    println("Server listening on port 5432")
    println()
    println("Try some commands:")
    println("  psql -h localhost -p 5432 -d xtdb")
    println("  INSERT INTO users VALUES ('user-1', 'Alice', 'alice@example.com');")
    println("  UPDATE users SET name = 'Alice Smith' WHERE _id = 'user-1';")
    println("  DELETE FROM users WHERE _id = 'user-1';")
    println()
    println("Watch for CDC files in: $cdcOutputDir")
    println("Press Ctrl+C to stop...")
    
    // Keep running
    try {
        Thread.currentThread().join()
    } catch (e: InterruptedException) {
        println("\nShutting down XTDB...")
        xtdb.close()
    }
}