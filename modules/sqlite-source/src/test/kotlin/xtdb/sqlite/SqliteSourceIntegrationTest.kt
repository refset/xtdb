package xtdb.sqlite

import kotlinx.coroutines.runInterruptible
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable
import org.junit.jupiter.api.io.TempDir
import org.testcontainers.kafka.ConfluentKafkaContainer
import org.testcontainers.lifecycle.Startables
import xtdb.api.Xtdb
import xtdb.api.log.KafkaCluster
import java.nio.file.Path
import java.sql.DriverManager
import java.util.UUID
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

@Tag("integration")
@EnabledIfEnvironmentVariable(named = "XTDB_SINGLE_WRITER", matches = "true")
class SqliteSourceIntegrationTest {

    companion object {
        private val kafka = ConfluentKafkaContainer("confluentinc/cp-kafka:7.8.0")

        @JvmStatic
        @BeforeAll
        fun beforeAll() {
            Startables.deepStart(kafka).join()
        }

        @JvmStatic
        @AfterAll
        fun afterAll() {
            kafka.stop()
        }
    }

    @TempDir
    lateinit var tempDir: Path

    private fun dbPath(name: String = "source.db"): String =
        tempDir.resolve(name).toAbsolutePath().toString()

    private fun sqliteJdbcUrl(path: String) = "jdbc:sqlite:$path"

    private fun sqliteExecute(path: String, vararg statements: String) {
        DriverManager.getConnection(sqliteJdbcUrl(path)).use { conn ->
            conn.createStatement().use { stmt ->
                for (sql in statements) stmt.execute(sql)
            }
        }
    }

    private fun openNode(sourceTopic: String, path: String): Xtdb = Xtdb.openNode {
        server { port = 0 }; flightSql = null
        logCluster("kafka", KafkaCluster.ClusterFactory(kafka.bootstrapServers))
        remote("sqlite", SqliteRemote.Factory(path = path))
        log(KafkaCluster.LogFactory("kafka", sourceTopic))
    }

    private fun attachSqliteSource(
        node: Xtdb,
        dbName: String = "cdc",
        tables: List<String>,
        cdcTable: String = "__xtdb_cdc_log__",
    ) {
        val tablesYaml = tables.joinToString(prefix = "[", postfix = "]")
        node.getConnection().use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute(
                    """
                    ATTACH DATABASE $dbName WITH $$
                        log: !Kafka
                          cluster: kafka
                          topic: test-replica-${UUID.randomUUID()}
                        externalSource: !Sqlite
                          remote: sqlite
                          cdcTable: $cdcTable
                          tableIncludeList: $tablesYaml
                    $$""".trimIndent()
                )
            }
        }
    }

    private fun xtQueryDb(node: Xtdb, dbName: String, sql: String): List<Map<String, Any?>> {
        return node.createConnectionBuilder().database(dbName).build().use { conn ->
            conn.createStatement().use { stmt ->
                stmt.executeQuery(sql).use { rs ->
                    val metadata = rs.metaData
                    val cols = (1..metadata.columnCount).map { metadata.getColumnName(it) }
                    buildList {
                        while (rs.next()) {
                            add(cols.associateWith { rs.getObject(it) })
                        }
                    }
                }
            }
        }
    }

    private suspend fun awaitTxs(node: Xtdb, expected: Int, db: String = "cdc", timeout: Duration = 10.seconds) {
        val deadline = System.currentTimeMillis() + timeout.inWholeMilliseconds
        var count = 0L
        while (System.currentTimeMillis() < deadline) {
            count = xtQueryDb(node, db, "SELECT count(*) AS cnt FROM xt.txs")[0]["cnt"] as Long
            if (count >= expected) return
            runInterruptible { Thread.sleep(200) }
        }
        throw AssertionError("Timed out waiting for $expected txs on db '$db' (got $count)")
    }

    private suspend fun awaitCondition(description: String, timeout: Duration = 15.seconds, check: () -> Boolean) {
        val deadline = System.currentTimeMillis() + timeout.inWholeMilliseconds

        while (System.currentTimeMillis() < deadline) {
            if (check()) return
            runInterruptible { Thread.sleep(200) }
        }

        fail("Timed out waiting for: $description")
    }

    @Test
    fun `snapshot and streaming CDC lifecycle`() = runTest(timeout = 120.seconds) {
        val path = dbPath()
        val sourceTopic = "test-topic-${UUID.randomUUID()}"

        sqliteExecute(
            path,
            "CREATE TABLE users (_id INTEGER PRIMARY KEY, name TEXT, email TEXT)",
            "INSERT INTO users (_id, name, email) VALUES (1, 'Alice', 'alice@example.com')",
        )

        openNode(sourceTopic, path).use { node ->
            attachSqliteSource(node, tables = listOf("users"))

            // 1 table batch + 1 completion marker = 2 txs
            awaitTxs(node, 2, db = "cdc")

            val snapshotRows = xtQueryDb(
                node, "cdc",
                "SELECT _id, name, email FROM public.users ORDER BY _id"
            )
            assertEquals(1, snapshotRows.size, "Snapshot should ingest Alice")
            assertEquals("alice@example.com", snapshotRows[0]["email"])

            // Streaming
            sqliteExecute(
                path,
                "INSERT INTO users (_id, name, email) VALUES (2, 'Bob', 'bob@example.com')",
                "UPDATE users SET email = 'alice-new@example.com' WHERE _id = 1",
            )

            awaitCondition("Alice updated", timeout = 30.seconds) {
                xtQueryDb(node, "cdc", "SELECT email FROM public.users WHERE _id = 1")
                    .firstOrNull()?.get("email") == "alice-new@example.com"
            }

            val bob = xtQueryDb(node, "cdc", "SELECT email FROM public.users WHERE _id = 2")
            assertEquals(1, bob.size)
            assertEquals("bob@example.com", bob[0]["email"])

            // Delete
            sqliteExecute(path, "DELETE FROM users WHERE _id = 2")

            awaitCondition("Bob deleted", timeout = 30.seconds) {
                xtQueryDb(node, "cdc", "SELECT _id FROM public.users WHERE _id = 2").isEmpty()
            }
        }
    }

    @Test
    fun `multi-table snapshot and streaming`() = runTest(timeout = 120.seconds) {
        val path = dbPath()
        val sourceTopic = "test-topic-${UUID.randomUUID()}"

        sqliteExecute(
            path,
            "CREATE TABLE mt_users (_id INTEGER PRIMARY KEY, name TEXT)",
            "CREATE TABLE mt_orders (_id INTEGER PRIMARY KEY, user_id INTEGER, amount REAL)",
            "INSERT INTO mt_users (_id, name) VALUES (1, 'Alice'), (2, 'Bob')",
            "INSERT INTO mt_orders (_id, user_id, amount) VALUES (1, 1, 99.99)",
        )

        openNode(sourceTopic, path).use { node ->
            attachSqliteSource(node, tables = listOf("mt_users", "mt_orders"))

            awaitCondition("both tables snapshotted", timeout = 30.seconds) {
                runCatching {
                    xtQueryDb(node, "cdc", "SELECT _id FROM public.mt_users").size == 2 &&
                        xtQueryDb(node, "cdc", "SELECT _id FROM public.mt_orders").size == 1
                }.getOrDefault(false)
            }

            val users = xtQueryDb(node, "cdc", "SELECT _id, name FROM public.mt_users ORDER BY _id")
            assertEquals("Alice", users[0]["name"])
            assertEquals("Bob", users[1]["name"])

            val orders = xtQueryDb(node, "cdc", "SELECT _id, user_id, amount FROM public.mt_orders")
            assertEquals(1, orders.size)

            // Stream changes to both tables
            sqliteExecute(
                path,
                "INSERT INTO mt_users (_id, name) VALUES (3, 'Charlie')",
                "INSERT INTO mt_orders (_id, user_id, amount) VALUES (2, 3, 42.00)",
            )

            awaitCondition("streaming changes to both tables", timeout = 30.seconds) {
                xtQueryDb(node, "cdc", "SELECT _id FROM public.mt_users WHERE _id = 3").isNotEmpty() &&
                    xtQueryDb(node, "cdc", "SELECT _id FROM public.mt_orders WHERE _id = 2").isNotEmpty()
            }
        }
    }

    @Test
    fun `schema evolution - add column during streaming`() = runTest(timeout = 120.seconds) {
        val path = dbPath()
        val sourceTopic = "test-topic-${UUID.randomUUID()}"

        sqliteExecute(
            path,
            "CREATE TABLE evolve (_id INTEGER PRIMARY KEY, name TEXT)",
            "INSERT INTO evolve (_id, name) VALUES (1, 'Alice')",
        )

        openNode(sourceTopic, path).use { node ->
            attachSqliteSource(node, tables = listOf("evolve"))

            awaitCondition("Alice snapshotted", timeout = 30.seconds) {
                runCatching {
                    xtQueryDb(node, "cdc", "SELECT _id FROM public.evolve WHERE _id = 1").isNotEmpty()
                }.getOrDefault(false)
            }

            // ALTER first and let the source detect + reinstall triggers on its next poll
            sqliteExecute(path, "ALTER TABLE evolve ADD COLUMN email TEXT")
            runInterruptible { Thread.sleep(1_500) }

            sqliteExecute(path, "INSERT INTO evolve (_id, name, email) VALUES (2, 'Bob', 'bob@example.com')")

            awaitCondition("Bob with new column appears", timeout = 30.seconds) {
                val rows = xtQueryDb(node, "cdc", "SELECT _id, email FROM public.evolve WHERE _id = 2")
                rows.isNotEmpty() && rows[0]["email"] == "bob@example.com"
            }

            // Alice (pre-evolution row) should have null for the new column
            val alice = xtQueryDb(node, "cdc", "SELECT _id, name, email FROM public.evolve WHERE _id = 1")
            assertEquals(1, alice.size)
            assertEquals(null, alice[0]["email"])
        }
    }

    @Test
    fun `resume from token after restart`() = runTest(timeout = 180.seconds) {
        val path = dbPath()
        val sourceTopic = "test-topic-${UUID.randomUUID()}"

        sqliteExecute(
            path,
            "CREATE TABLE resume_t (_id INTEGER PRIMARY KEY, name TEXT)",
            "INSERT INTO resume_t (_id, name) VALUES (1, 'Alice')",
        )

        // Phase 1: snapshot + streaming
        openNode(sourceTopic, path).use { node ->
            attachSqliteSource(node, tables = listOf("resume_t"))

            awaitTxs(node, 2, db = "cdc")

            val snapshotRows = xtQueryDb(node, "cdc", "SELECT _id, name FROM public.resume_t ORDER BY _id")
            assertEquals(1, snapshotRows.size)
            assertEquals("Alice", snapshotRows[0]["name"])

            // Stream an insert so the token advances beyond the snapshot seq
            sqliteExecute(path, "INSERT INTO resume_t (_id, name) VALUES (2, 'Bob')")

            awaitCondition("Bob appears", timeout = 30.seconds) {
                xtQueryDb(node, "cdc", "SELECT _id FROM public.resume_t WHERE _id = 2").isNotEmpty()
            }
        }

        // Insert while node is down — this must appear after restart.
        // Triggers installed by the previous run are still active, so the CDC log captures this.
        sqliteExecute(path, "INSERT INTO resume_t (_id, name) VALUES (3, 'Charlie')")

        // Phase 2: restart with the same Kafka source topic and the same SQLite file.
        openNode(sourceTopic, path).use { node ->
            awaitCondition("Charlie appears after restart", timeout = 60.seconds) {
                runCatching {
                    xtQueryDb(node, "cdc", "SELECT _id FROM public.resume_t WHERE _id = 3").isNotEmpty()
                }.getOrDefault(false)
            }

            val rows = xtQueryDb(node, "cdc", "SELECT _id, name FROM public.resume_t ORDER BY _id")
            assertEquals(3, rows.size, "All three rows should be present — no duplication, no loss")
            assertEquals("Alice", rows[0]["name"])
            assertEquals("Bob", rows[1]["name"])
            assertEquals("Charlie", rows[2]["name"])
        }
    }

    @Test
    fun `partial snapshot failure when CDC log already has rows`() = runTest(timeout = 120.seconds) {
        val path = dbPath()
        val sourceTopic = "test-topic-${UUID.randomUUID()}"

        sqliteExecute(
            path,
            "CREATE TABLE partial (_id INTEGER PRIMARY KEY, name TEXT)",
            "INSERT INTO partial (_id, name) VALUES (1, 'Alice')",
            // Pre-create the CDC log with a stale row — simulates a prior crash mid-snapshot.
            """
            CREATE TABLE __xtdb_cdc_log__ (
                seq INTEGER PRIMARY KEY AUTOINCREMENT,
                tbl TEXT NOT NULL,
                op TEXT NOT NULL,
                data TEXT NOT NULL
            )""".trimIndent(),
            "INSERT INTO __xtdb_cdc_log__ (tbl, op, data) VALUES ('partial', 'c', '{\"_id\": 1}')",
        )

        openNode(sourceTopic, path).use { node ->
            attachSqliteSource(node, tables = listOf("partial"))

            // Snapshot should refuse to start because CDC log isn't empty. Wait for the attempt.
            runInterruptible { Thread.sleep(5_000) }

            val result = runCatching {
                xtQueryDb(node, "cdc", "SELECT _id FROM public.partial")
            }

            assertTrue(
                result.isFailure || result.getOrNull().isNullOrEmpty(),
                "Snapshot should fail when CDC log has pre-existing rows — expected error or no data, got: ${result.getOrNull()}"
            )
        }
    }

    @Test
    fun `cancellation closes connection and leaves DB writable`() = runTest(timeout = 120.seconds) {
        val path = dbPath()
        val sourceTopic = "test-topic-${UUID.randomUUID()}"

        sqliteExecute(
            path,
            "CREATE TABLE cancel_t (_id INTEGER PRIMARY KEY, name TEXT)",
            "INSERT INTO cancel_t (_id, name) VALUES (1, 'Alice')",
        )

        openNode(sourceTopic, path).use { node ->
            attachSqliteSource(node, tables = listOf("cancel_t"))
            awaitTxs(node, 2, db = "cdc")

            // Streaming is running and holding its JDBC connection.
            sqliteExecute(path, "INSERT INTO cancel_t (_id, name) VALUES (2, 'Bob')")
            awaitCondition("Bob appears", timeout = 30.seconds) {
                xtQueryDb(node, "cdc", "SELECT _id FROM public.cancel_t WHERE _id = 2").isNotEmpty()
            }
        }
        // node.close() cancels extJob; the streaming loop's suspend call exits and its
        // connection is closed via `.use`. The DB file remains usable.

        val rows = DriverManager.getConnection(sqliteJdbcUrl(path)).use { conn ->
            conn.createStatement().use { stmt ->
                stmt.executeQuery("SELECT count(*) AS cnt FROM cancel_t").use { rs ->
                    rs.next(); rs.getLong("cnt")
                }
            }
        }
        assertEquals(2L, rows)

        // Further writes succeed — no dangling lock from the cancelled source.
        sqliteExecute(path, "INSERT INTO cancel_t (_id, name) VALUES (3, 'Charlie')")

        val count = DriverManager.getConnection(sqliteJdbcUrl(path)).use { conn ->
            conn.createStatement().use { stmt ->
                stmt.executeQuery("SELECT count(*) AS cnt FROM cancel_t").use { rs ->
                    rs.next(); rs.getLong("cnt")
                }
            }
        }
        assertEquals(3L, count)
    }
}
