package xtdb.sqlite

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class SqliteJsonRowTest {

    @Test
    fun `parses sqlite json_object output to typed row`() {
        // This mirrors what `json_object('_id', NEW._id, 'name', NEW.name, 'age', NEW.age,
        // 'active', NEW.active, 'score', NEW.score, 'bio', NEW.bio)` produces for a row with
        // integer / text / boolean-like / real / null values.
        val json = """{"_id": 1, "name": "Alice", "age": 42, "active": 1, "score": 3.14, "bio": null}"""
        val row = SqliteSource.parseJsonRow(json)

        assertEquals(1L, row["_id"])
        assertEquals("Alice", row["name"])
        assertEquals(42L, row["age"])
        // SQLite has no bool; json_object renders it as the integer the user stored.
        assertEquals(1L, row["active"])
        assertEquals(3.14, row["score"])
        assertEquals(null, row["bio"])
        assertEquals(true, row.containsKey("bio"))
    }
}
