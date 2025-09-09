package xtdb.kafka.cdc

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.apache.avro.Schema

class SimpleSchemaEvolutionTest {
    
    private lateinit var schemaManager: SimpleSchemaManager
    
    @BeforeEach
    fun setup() {
        schemaManager = SimpleSchemaManager()
    }
    
    @Test
    fun `test simple schema manager returns fixed schema`() {
        val table = "users"
        
        val schema = schemaManager.getSchema(table)
        
        assertNotNull(schema)
        assertEquals("Envelope", schema.name)
        assertEquals("xtdb.kafka.cdc", schema.namespace)
        
        // Verify essential fields exist
        val fieldNames = schema.fields.map { it.name() }.toSet()
        assertTrue(fieldNames.contains("before"))
        assertTrue(fieldNames.contains("after")) 
        assertTrue(fieldNames.contains("source"))
        assertTrue(fieldNames.contains("op"))
    }
    
    @Test
    fun `test schema is consistent across calls`() {
        val table1 = "users"
        val table2 = "products"
        
        val schema1 = schemaManager.getSchema(table1)
        val schema2 = schemaManager.getSchema(table2)
        
        // Should return the same schema for all tables
        assertEquals(schema1, schema2)
    }
    
    @Test
    fun `test schema structure for JSON string approach`() {
        val schema = schemaManager.getSchema("test_table")
        
        // Verify before and after fields are nullable string unions
        val beforeField = schema.getField("before")
        val afterField = schema.getField("after")
        
        assertTrue(beforeField.schema().isUnion)
        assertTrue(afterField.schema().isUnion)
        
        val beforeTypes = beforeField.schema().types.map { it.type }
        val afterTypes = afterField.schema().types.map { it.type }
        
        assertTrue(beforeTypes.contains(Schema.Type.NULL))
        assertTrue(beforeTypes.contains(Schema.Type.STRING))
        assertTrue(afterTypes.contains(Schema.Type.NULL)) 
        assertTrue(afterTypes.contains(Schema.Type.STRING))
    }
}