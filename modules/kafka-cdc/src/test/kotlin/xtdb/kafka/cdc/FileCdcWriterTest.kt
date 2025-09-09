@file:OptIn(kotlin.io.path.ExperimentalPathApi::class)

package xtdb.kafka.cdc

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.AfterEach
import java.nio.file.Files
import java.nio.file.Paths
import java.time.Instant
import java.util.UUID
import kotlin.io.path.deleteRecursively
import kotlin.io.path.exists

class FileCdcWriterTest {
    
    private val testOutputDir = Paths.get("build/test-cdc-output-${UUID.randomUUID()}")
    private lateinit var fileCdcWriter: FileCdcWriter
    
    @BeforeEach
    fun setup() {
        fileCdcWriter = FileCdcWriter(testOutputDir, prettyPrint = true)
    }
    
    @AfterEach
    fun tearDown() {
        fileCdcWriter.close()
        if (testOutputDir.exists()) {
            testOutputDir.deleteRecursively()
        }
    }
    
    @Test
    fun `test write sample CDC events to files`() {
        val now = Instant.now()
        
        // CREATE event - new user
        fileCdcWriter.writeCdcEvent(CdcEvent(
            database = "xtdb",
            tableName = "users",
            recordId = "user-123",
            operation = CdcOperation.CREATE,
            beforeValue = null,
            afterValue = """{"_id": "user-123", "name": "Alice Smith", "email": "alice@example.com", "created_at": "${now}"}""",
            timestamp = now,
            systemTime = now,
            txId = 1001L,
            transactionInfo = TransactionInfo("tx-1001", 1001L, 1L)
        ))
        
        // UPDATE event - user changes email
        fileCdcWriter.writeCdcEvent(CdcEvent(
            database = "xtdb",
            tableName = "users",
            recordId = "user-123",
            operation = CdcOperation.UPDATE,
            beforeValue = """{"_id": "user-123", "name": "Alice Smith", "email": "alice@example.com", "created_at": "${now}"}""",
            afterValue = """{"_id": "user-123", "name": "Alice Smith", "email": "alice.smith@newdomain.com", "created_at": "${now}", "updated_at": "${now.plusSeconds(3600)}"}""",
            timestamp = now.plusSeconds(3600),
            systemTime = now.plusSeconds(3600),
            txId = 1002L,
            transactionInfo = TransactionInfo("tx-1002", 1002L, 1L)
        ))
        
        // CREATE event - new product
        fileCdcWriter.writeCdcEvent(CdcEvent(
            database = "xtdb",
            tableName = "products", 
            recordId = "prod-456",
            operation = CdcOperation.CREATE,
            beforeValue = null,
            afterValue = """{"_id": "prod-456", "name": "Widget Pro", "price": 29.99, "category": "widgets", "in_stock": true, "quantity": 100}""",
            timestamp = now.plusSeconds(1800),
            systemTime = now.plusSeconds(1800),
            txId = 1003L,
            transactionInfo = TransactionInfo("tx-1003", 1003L, 1L)
        ))
        
        // UPDATE event - price change
        fileCdcWriter.writeCdcEvent(CdcEvent(
            database = "xtdb",
            tableName = "products",
            recordId = "prod-456",
            operation = CdcOperation.UPDATE,
            beforeValue = """{"_id": "prod-456", "name": "Widget Pro", "price": 29.99, "category": "widgets", "in_stock": true, "quantity": 100}""",
            afterValue = """{"_id": "prod-456", "name": "Widget Pro", "price": 24.99, "category": "widgets", "in_stock": true, "quantity": 85, "on_sale": true}""",
            timestamp = now.plusSeconds(7200),
            systemTime = now.plusSeconds(7200),
            txId = 1004L,
            transactionInfo = TransactionInfo("tx-1004", 1004L, 1L)
        ))
        
        // DELETE event - remove user
        fileCdcWriter.writeCdcEvent(CdcEvent(
            database = "xtdb",
            tableName = "users",
            recordId = "user-999",
            operation = CdcOperation.DELETE,
            beforeValue = """{"_id": "user-999", "name": "Deleted User", "email": "deleted@example.com"}""",
            afterValue = null,
            timestamp = now.plusSeconds(10800),
            systemTime = now.plusSeconds(10800),
            txId = 1005L,
            transactionInfo = TransactionInfo("tx-1005", 1005L, 1L)
        ))
        
        // Verify files were created
        assertTrue(Files.exists(testOutputDir))
        
        val usersDir = testOutputDir.resolve("users")
        assertTrue(Files.exists(usersDir))
        assertEquals(3, Files.list(usersDir).count()) // 2 user events + 1 delete
        
        val productsDir = testOutputDir.resolve("products")
        assertTrue(Files.exists(productsDir))
        assertEquals(2, Files.list(productsDir).count()) // 2 product events
        
        // Check summary was created
        fileCdcWriter.close()
        val summaryFile = testOutputDir.resolve("cdc-summary.txt")
        assertTrue(Files.exists(summaryFile))
        
        val summaryContent = Files.readString(summaryFile)
        assertTrue(summaryContent.contains("Total events: 5"))
        assertTrue(summaryContent.contains("users: 3 events"))
        assertTrue(summaryContent.contains("products: 2 events"))
        
        println("\n=== CDC Output Created ===")
        println("Check the output at: ${testOutputDir.toAbsolutePath()}")
        println("You can inspect the JSON files to see the CDC events in Debezium format")
    }
    
    @Test
    fun `test complex nested document`() {
        val complexDoc = mapOf(
            "_id" to "order-789",
            "customer" to mapOf(
                "id" to "cust-123",
                "name" to "John Doe",
                "addresses" to listOf(
                    mapOf("type" to "billing", "street" to "123 Main St", "city" to "Springfield"),
                    mapOf("type" to "shipping", "street" to "456 Oak Ave", "city" to "Riverside")
                )
            ),
            "items" to listOf(
                mapOf("product_id" to "prod-1", "quantity" to 2, "price" to 19.99),
                mapOf("product_id" to "prod-2", "quantity" to 1, "price" to 49.99)
            ),
            "metadata" to mapOf(
                "created_at" to Instant.now().toString(),
                "source" to "web",
                "session_id" to UUID.randomUUID().toString()
            )
        )
        
        val objectMapper = com.fasterxml.jackson.databind.ObjectMapper()
        val jsonString = objectMapper.writeValueAsString(complexDoc)
        
        fileCdcWriter.writeCdcEvent(CdcEvent(
            database = "xtdb",
            tableName = "orders",
            recordId = "order-789",
            operation = CdcOperation.CREATE,
            beforeValue = null,
            afterValue = jsonString,
            timestamp = Instant.now(),
            systemTime = Instant.now(),
            txId = 2001L,
            transactionInfo = TransactionInfo("tx-2001", 2001L, 1L)
        ))
        
        val ordersDir = testOutputDir.resolve("orders")
        assertTrue(Files.exists(ordersDir))
        assertEquals(1, Files.list(ordersDir).count())
        
        // Read and verify the JSON structure
        val jsonFile = Files.list(ordersDir).findFirst().get()
        val content = Files.readString(jsonFile)
        assertTrue(content.contains("\"op\" : \"c\"")) // CREATE operation
        assertTrue(content.contains("\"table\" : \"orders\""))
        assertTrue(content.contains("order-789"))
    }
}