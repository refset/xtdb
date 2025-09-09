package xtdb.kafka.cdc

import java.nio.file.Paths
import java.time.Instant
import java.time.LocalDate
import java.time.Duration
import java.util.UUID
import java.math.BigDecimal

/**
 * Example program that generates sample CDC events to files for inspection.
 * Run this to see what the CDC output looks like.
 */
fun main() {
    val outputDir = Paths.get("cdc-output-example")
    println("Generating sample CDC events to: ${outputDir.toAbsolutePath()}")
    
    FileCdcWriter(outputDir, prettyPrint = true).use { writer ->
        
        val baseTime = Instant.now()
        var txId = 1000L
        
        // Scenario 1: User lifecycle
        println("\n=== User Lifecycle Events ===")
        
        // User signs up
        writer.writeCdcEvent(CdcEvent(
            database = "xtdb",
            tableName = "users",
            recordId = "user-alice",
            operation = CdcOperation.CREATE,
            beforeValue = null,
            afterValue = """
                {
                    "_id": "user-alice",
                    "username": "alice",
                    "email": "alice@example.com",
                    "full_name": "Alice Johnson",
                    "created_at": "${baseTime}",
                    "status": "active",
                    "preferences": {
                        "theme": "dark",
                        "notifications": true,
                        "language": "en"
                    }
                }
            """.trimIndent(),
            timestamp = baseTime,
            systemTime = baseTime,
            txId = txId++,
            transactionInfo = TransactionInfo("tx-${txId-1}", txId-1, 1L)
        ))
        
        // User updates profile
        writer.writeCdcEvent(CdcEvent(
            database = "xtdb",
            tableName = "users",
            recordId = "user-alice",
            operation = CdcOperation.UPDATE,
            beforeValue = """
                {
                    "_id": "user-alice",
                    "username": "alice",
                    "email": "alice@example.com",
                    "full_name": "Alice Johnson",
                    "created_at": "${baseTime}",
                    "status": "active",
                    "preferences": {
                        "theme": "dark",
                        "notifications": true,
                        "language": "en"
                    }
                }
            """.trimIndent(),
            afterValue = """
                {
                    "_id": "user-alice",
                    "username": "alice",
                    "email": "alice@example.com",
                    "full_name": "Alice Johnson-Smith",
                    "created_at": "${baseTime}",
                    "updated_at": "${baseTime.plusSeconds(3600)}",
                    "status": "active",
                    "bio": "Software engineer and coffee enthusiast",
                    "location": "San Francisco, CA",
                    "preferences": {
                        "theme": "dark",
                        "notifications": true,
                        "language": "en",
                        "timezone": "America/Los_Angeles"
                    },
                    "social": {
                        "twitter": "@alice_codes",
                        "github": "alice-dev"
                    }
                }
            """.trimIndent(),
            timestamp = baseTime.plusSeconds(3600),
            systemTime = baseTime.plusSeconds(3600),
            txId = txId++,
            transactionInfo = TransactionInfo("tx-${txId-1}", txId-1, 1L)
        ))
        
        // Scenario 2: E-commerce order with complex types
        println("\n=== E-commerce Order Events ===")
        
        writer.writeCdcEvent(CdcEvent(
            database = "xtdb",
            tableName = "orders",
            recordId = "order-2024-001",
            operation = CdcOperation.CREATE,
            beforeValue = null,
            afterValue = """
                {
                    "_id": "order-2024-001",
                    "order_number": "ORD-2024-001",
                    "customer_id": "user-alice",
                    "order_date": "${LocalDate.now()}",
                    "status": "pending",
                    "items": [
                        {
                            "product_id": "prod-laptop-01",
                            "product_name": "ThinkPad X1 Carbon",
                            "quantity": 1,
                            "unit_price": 1299.99,
                            "discount_percent": 10,
                            "final_price": 1169.99
                        },
                        {
                            "product_id": "prod-mouse-02",
                            "product_name": "Wireless Mouse",
                            "quantity": 2,
                            "unit_price": 29.99,
                            "discount_percent": 0,
                            "final_price": 59.98
                        }
                    ],
                    "pricing": {
                        "subtotal": 1229.97,
                        "tax_rate": 0.0875,
                        "tax_amount": 107.62,
                        "shipping": 0.00,
                        "total": 1337.59,
                        "currency": "USD"
                    },
                    "shipping_address": {
                        "name": "Alice Johnson-Smith",
                        "street": "123 Tech Lane",
                        "city": "San Francisco",
                        "state": "CA",
                        "zip": "94105",
                        "country": "USA"
                    },
                    "metadata": {
                        "source": "web",
                        "ip_address": "192.168.1.42",
                        "user_agent": "Mozilla/5.0...",
                        "session_id": "${UUID.randomUUID()}",
                        "processing_time_ms": 247
                    }
                }
            """.trimIndent(),
            timestamp = baseTime.plusSeconds(7200),
            systemTime = baseTime.plusSeconds(7200),
            txId = txId++,
            transactionInfo = TransactionInfo("tx-${txId-1}", txId-1, 1L)
        ))
        
        // Order status update
        writer.writeCdcEvent(CdcEvent(
            database = "xtdb",
            tableName = "orders",
            recordId = "order-2024-001",
            operation = CdcOperation.UPDATE,
            beforeValue = """{"_id": "order-2024-001", "status": "pending"}""",
            afterValue = """
                {
                    "_id": "order-2024-001",
                    "status": "shipped",
                    "shipped_at": "${baseTime.plusSeconds(86400)}",
                    "tracking_number": "1Z999AA10123456784",
                    "carrier": "UPS",
                    "estimated_delivery": "${baseTime.plusSeconds(259200)}"
                }
            """.trimIndent(),
            timestamp = baseTime.plusSeconds(86400),
            systemTime = baseTime.plusSeconds(86400),
            txId = txId++,
            transactionInfo = TransactionInfo("tx-${txId-1}", txId-1, 1L)
        ))
        
        // Scenario 3: Product with XTDB/Arrow types
        println("\n=== Product with Special Types ===")
        
        val arrowSerializer = ArrowSerializer()
        
        writer.writeCdcEvent(CdcEvent(
            database = "xtdb",
            tableName = "products",
            recordId = "prod-special-01",
            operation = CdcOperation.CREATE,
            beforeValue = null,
            afterValue = """
                {
                    "_id": "prod-special-01",
                    "sku": "SKU-2024-SPEC",
                    "name": "Premium Widget",
                    "description": "A widget with special Arrow type fields",
                    "price": "999.99",
                    "price_decimal": "${arrowSerializer.serialize(BigDecimal("999.99"))}",
                    "created_instant": "${arrowSerializer.serialize(Instant.now())}",
                    "availability_date": "${arrowSerializer.serialize(LocalDate.now().plusDays(7))}",
                    "warranty_duration": "${arrowSerializer.serialize(Duration.ofDays(365))}",
                    "product_uuid": "${arrowSerializer.serialize(UUID.randomUUID())}",
                    "specifications": {
                        "weight_kg": 2.5,
                        "dimensions_cm": [30, 20, 10],
                        "materials": ["aluminum", "carbon fiber", "glass"],
                        "certifications": ["CE", "FCC", "RoHS"]
                    },
                    "binary_data": "${arrowSerializer.serialize(byteArrayOf(1, 2, 3, 4, 5))}",
                    "tags": ["premium", "limited-edition", "2024"],
                    "metadata": {
                        "internal_id": "${UUID.randomUUID()}",
                        "revision": 1,
                        "last_modified": "${Instant.now()}"
                    }
                }
            """.trimIndent(),
            timestamp = baseTime.plusSeconds(10800),
            systemTime = baseTime.plusSeconds(10800),
            txId = txId++,
            transactionInfo = TransactionInfo("tx-${txId-1}", txId-1, 1L)
        ))
        
        // Scenario 4: Deletion
        println("\n=== Deletion Event ===")
        
        writer.writeCdcEvent(CdcEvent(
            database = "xtdb",
            tableName = "temp_sessions",
            recordId = "session-expired-123",
            operation = CdcOperation.DELETE,
            beforeValue = """
                {
                    "_id": "session-expired-123",
                    "user_id": "user-bob",
                    "created_at": "${baseTime.minusSeconds(7200)}",
                    "last_activity": "${baseTime.minusSeconds(3600)}",
                    "ip_address": "10.0.0.1",
                    "expired": true
                }
            """.trimIndent(),
            afterValue = null,
            timestamp = baseTime.plusSeconds(14400),
            systemTime = baseTime.plusSeconds(14400),
            txId = txId++,
            transactionInfo = TransactionInfo("tx-${txId-1}", txId-1, 1L)
        ))
        
        // Scenario 5: Snapshot read
        println("\n=== Snapshot Event ===")
        
        writer.writeCdcEvent(CdcEvent(
            database = "xtdb",
            tableName = "config",
            recordId = "config-global",
            operation = CdcOperation.READ,
            beforeValue = null,
            afterValue = """
                {
                    "_id": "config-global",
                    "version": "2.0.0",
                    "features": {
                        "cdc_enabled": true,
                        "audit_logging": true,
                        "rate_limiting": {
                            "enabled": true,
                            "requests_per_minute": 1000
                        }
                    },
                    "maintenance_mode": false,
                    "last_updated": "${baseTime}"
                }
            """.trimIndent(),
            timestamp = baseTime,
            systemTime = baseTime,
            txId = 0L,
            isSnapshot = true,
            transactionInfo = null
        ))
    }
    
    println("\n" + "=".repeat(50))
    println("✅ CDC events have been written to: ${outputDir.toAbsolutePath()}")
    println("\nYou can now inspect the JSON files in the output directory.")
    println("Each file contains a complete Debezium-compatible CDC event.")
    println("\nDirectory structure:")
    println("  ${outputDir.fileName}/")
    println("    ├── users/         (user lifecycle events)")
    println("    ├── orders/        (order events)")  
    println("    ├── products/      (product with special types)")
    println("    ├── temp_sessions/ (deletion event)")
    println("    ├── config/        (snapshot event)")
    println("    └── cdc-summary.txt")
    println("=".repeat(50))
}