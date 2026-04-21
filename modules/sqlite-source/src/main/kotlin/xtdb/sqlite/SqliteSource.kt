package xtdb.sqlite

import kotlinx.coroutines.*
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.longOrNull
import kotlinx.serialization.modules.PolymorphicModuleBuilder
import kotlinx.serialization.modules.subclass
import xtdb.api.Remote
import xtdb.api.RemoteAlias
import xtdb.api.log.Log
import xtdb.api.log.LogClusterAlias
import xtdb.database.ExternalSource
import xtdb.database.ExternalSourceToken
import xtdb.database.proto.DatabaseConfig
import xtdb.error.Incorrect
import xtdb.indexer.TxIndexer
import xtdb.indexer.TxIndexer.OpenTx
import xtdb.indexer.TxIndexer.TxResult
import xtdb.sqlite.proto.SqliteSourceConfig
import xtdb.sqlite.proto.SqliteSourceToken
import xtdb.sqlite.proto.sqliteSourceConfig
import xtdb.sqlite.proto.sqliteSourceToken
import xtdb.table.TableRef
import xtdb.time.InstantUtil.asMicros
import xtdb.util.*
import java.nio.ByteBuffer
import java.sql.Connection
import java.sql.DriverManager
import java.time.Duration
import java.time.Instant
import com.google.protobuf.Any as ProtoAny

private val LOG = SqliteSource::class.logger

private const val PROTO_TAG = "proto.xtdb.com"
private const val SNAPSHOT_BATCH_SIZE = 1000
private const val STREAM_BATCH_SIZE = 500
private const val DEFAULT_CDC_TABLE = "__xtdb_cdc_log__"

class SqliteSource(
    private val dbName: String,
    private val path: String,
    private val cdcTable: String,
    private val tableIncludeList: List<String>,
    private val pollDuration: Duration = Duration.ofMillis(250),
) : ExternalSource {

    @Serializable
    @SerialName("!Sqlite")
    data class Factory(
        val remote: RemoteAlias,
        val cdcTable: String = DEFAULT_CDC_TABLE,
        val tableIncludeList: List<String>,
    ) : ExternalSource.Factory {

        override fun open(
            dbName: String,
            clusters: Map<LogClusterAlias, Log.Cluster>,
            remotes: Map<RemoteAlias, Remote>,
        ): ExternalSource {
            val raw = remotes[remote]
                ?: throw Incorrect(
                    "no remote configured with alias '$remote' — add a '!Sqlite' entry under 'remotes:' in node config",
                    errorCode = "xtdb.sqlite/missing-remote",
                    data = mapOf("alias" to remote),
                )

            val actualType = raw::class.simpleName ?: raw::class.qualifiedName ?: "unknown"

            val sqlite = raw as? SqliteRemote
                ?: throw Incorrect(
                    "remote '$remote' is a $actualType, expected a !Sqlite remote",
                    errorCode = "xtdb.sqlite/wrong-remote-type",
                    data = mapOf("alias" to remote, "actualType" to actualType),
                )

            return SqliteSource(dbName, sqlite.path, cdcTable, tableIncludeList)
        }

        override fun writeTo(dbConfig: DatabaseConfig.Builder) {
            dbConfig.externalSource = ProtoAny.pack(sqliteSourceConfig {
                remote = this@Factory.remote
                cdcTable = this@Factory.cdcTable
                tableIncludeList += this@Factory.tableIncludeList
            }, PROTO_TAG)
        }

        class Registration : ExternalSource.Registration {
            override val protoTag: String get() = "$PROTO_TAG/xtdb.sqlite.proto.SqliteSourceConfig"

            override fun fromProto(msg: ProtoAny): ExternalSource.Factory {
                val config = msg.unpack(SqliteSourceConfig::class.java)
                return Factory(
                    remote = config.remote,
                    cdcTable = config.cdcTable.ifEmpty { DEFAULT_CDC_TABLE },
                    tableIncludeList = config.tableIncludeListList,
                )
            }

            override fun registerSerde(builder: PolymorphicModuleBuilder<ExternalSource.Factory>) {
                builder.subclass(Factory::class)
            }
        }
    }

    private fun openJdbcConnection(): Connection =
        DriverManager.getConnection("jdbc:sqlite:$path").also { conn ->
            conn.createStatement().use { stmt ->
                // WAL mode keeps readers non-blocking and lets external writers proceed while
                // we hold BEGIN IMMEDIATE for the snapshot.
                stmt.execute("PRAGMA journal_mode=WAL")
                stmt.execute("PRAGMA foreign_keys=ON")
            }
        }

    override suspend fun onPartitionAssigned(
        partition: Int,
        afterToken: ExternalSourceToken?,
        txIndexer: TxIndexer,
    ) {
        LOG.info("[$dbName] Partition $partition assigned (path=$path, cdcTable=$cdcTable, tables=${tableIncludeList.joinToString()})")

        val token = afterToken?.unpack(SqliteSourceToken::class.java)
        LOG.debug { "[$dbName] Recovered token: ${token ?: "none"}" }

        try {
            if (token != null && token.snapshotCompleted) {
                LOG.info("[$dbName] Resuming streaming from seq ${token.latestSeq}")
                streamChanges(txIndexer, token.latestSeq)
            } else {
                LOG.info(
                    "[$dbName] Starting initial snapshot" +
                            if (token != null) " (recovering from partial snapshot)" else ""
                )
                val resumeSeq = initialSnapshot(txIndexer, recovering = token != null)
                LOG.info("[$dbName] Snapshot complete, switching to streaming from seq $resumeSeq")
                streamChanges(txIndexer, resumeSeq)
            }
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            LOG.error(e, "[$dbName] External source failed")
            throw e
        }
    }

    /**
     * Returns the seq to resume streaming from (i.e. the [cdcTable]'s MAX(seq) at snapshot time).
     *
     * Ordering matters for crash-consistency:
     * 1. `BEGIN IMMEDIATE` → create cdcTable → install triggers → materialize snapshot rows → `COMMIT`.
     *    SQLite durability is established *before* any xtdb writes. This way, a crash here leaves
     *    xtdb with no snapshot-complete marker; on restart we detect the missing completion,
     *    treat the CDC log's contents as unreliable partial state, and restart the snapshot.
     * 2. Stream materialized rows to xtdb in batches (token: `snapshotCompleted=false`).
     * 3. Write the completion marker (token: `snapshotCompleted=true`).
     *    Crash between steps 2 and 3 is recoverable: next run re-enters initialSnapshot with
     *    `recovering=true`, wipes partial CDC rows, re-snapshots (rows overlay in xtdb).
     *
     * [recovering]=true skips the empty-CDC assertion and wipes the CDC log so the retry can
     * run against a clean slate. First-run callers pass `false` so accidental pre-existing CDC
     * state is rejected instead of silently destroyed.
     */
    private suspend fun initialSnapshot(txIndexer: TxIndexer, recovering: Boolean): Long {
        LOG.debug { "[$dbName] Opening snapshot connection to $path" }

        data class TableSnapshot(val table: String, val rows: List<Map<String, Any?>>)

        val (resumeSeq, snapshots) = runInterruptible(Dispatchers.IO) {
            openJdbcConnection().use { c ->
                beginImmediate(c)
                try {
                    ensureCdcTable(c)
                    if (recovering) {
                        val deleted = c.createStatement().use { stmt ->
                            stmt.executeUpdate("DELETE FROM \"$cdcTable\"")
                        }
                        if (deleted > 0)
                            LOG.info("[$dbName] Recovery: cleared $deleted stale CDC log rows from partial snapshot")
                    } else {
                        assertCdcLogEmpty(c)
                    }

                    for (table in tableIncludeList) installTriggers(c, table)

                    val resumeSeq = readMaxSeq(c)
                    val snapshots = tableIncludeList.map { table ->
                        val columns = readColumns(c, table)
                        LOG.info("[$dbName] Snapshotting $table (${columns.size} columns: ${columns.joinToString()})")
                        val rows = readTableRows(c, table, columns)
                        LOG.info("[$dbName] Read ${rows.size} rows from $table")
                        TableSnapshot(table, rows)
                    }

                    c.createStatement().use { it.execute("COMMIT") }
                    resumeSeq to snapshots
                } catch (e: Throwable) {
                    runCatching { c.createStatement().use { it.execute("ROLLBACK") } }
                    throw e
                }
            }
        }

        for (snap in snapshots) {
            snap.rows.chunked(SNAPSHOT_BATCH_SIZE).forEach { batch ->
                LOG.debug { "[$dbName] Flushing ${snap.table} batch: ${batch.size} rows" }
                flushSnapshotBatch(txIndexer, resumeSeq, snap.table, batch)
            }
        }

        val completeToken = ProtoAny.pack(sqliteSourceToken {
            latestSeq = resumeSeq
            snapshotCompleted = true
        }, PROTO_TAG)

        LOG.debug { "[$dbName] Writing snapshot-complete marker" }
        txIndexer.indexTx(completeToken) { TxResult.Committed() }

        return resumeSeq
    }

    private suspend fun streamChanges(txIndexer: TxIndexer, afterSeq: Long) {
        LOG.debug { "[$dbName] Opening streaming connection" }

        var latestSeq = afterSeq
        val cachedCols = mutableMapOf<String, List<String>>()
        val conn = runInterruptible(Dispatchers.IO) { openJdbcConnection() }

        conn.use { c ->
            try {
                while (currentCoroutineContext().isActive) {
                    runInterruptible(Dispatchers.IO) { refreshTriggersIfChanged(c, cachedCols) }

                    val batch = runInterruptible(Dispatchers.IO) { readCdcBatch(c, latestSeq) }

                    if (batch.isEmpty()) {
                        delay(pollDuration.toMillis())
                        continue
                    }

                    LOG.trace { "[$dbName] Streaming batch of ${batch.size} CDC rows (seqs ${batch.first().seq}..${batch.last().seq})" }

                    val batchMaxSeq = batch.last().seq
                    val token = ProtoAny.pack(sqliteSourceToken {
                        latestSeq = batchMaxSeq
                        snapshotCompleted = true
                    }, PROTO_TAG)

                    txIndexer.indexTx(token) { openTx ->
                        for (row in batch) applyStreamingOp(openTx, row)
                        TxResult.Committed()
                    }

                    runInterruptible(Dispatchers.IO) { trimCdcLog(c, batchMaxSeq) }
                    latestSeq = batchMaxSeq
                }
            } finally {
                LOG.info("[$dbName] Streaming loop exiting")
            }
        }
    }

    private data class CdcRow(val seq: Long, val tbl: String, val op: String, val data: String)

    private fun readCdcBatch(conn: Connection, afterSeq: Long): List<CdcRow> =
        conn.prepareStatement(
            "SELECT seq, tbl, op, data FROM \"$cdcTable\" WHERE seq > ? ORDER BY seq LIMIT ?"
        ).use { ps ->
            ps.setLong(1, afterSeq)
            ps.setInt(2, STREAM_BATCH_SIZE)
            ps.executeQuery().use { rs ->
                buildList {
                    while (rs.next()) {
                        add(CdcRow(rs.getLong("seq"), rs.getString("tbl"), rs.getString("op"), rs.getString("data")))
                    }
                }
            }
        }

    private fun trimCdcLog(conn: Connection, throughSeq: Long) {
        conn.prepareStatement("DELETE FROM \"$cdcTable\" WHERE seq <= ?").use { ps ->
            ps.setLong(1, throughSeq)
            ps.executeUpdate()
        }
    }

    private fun applyStreamingOp(openTx: OpenTx, cdc: CdcRow) {
        val row = parseJsonRow(cdc.data)
        val schema = "public"

        when (cdc.op) {
            "c", "u" -> writeRow(openTx, dbName, schema, cdc.tbl, cdc.op, row, null)
            "d" -> writeRow(openTx, dbName, schema, cdc.tbl, "d", null, row)
            else -> throw Incorrect("Unknown CDC op '${cdc.op}' for table '${cdc.tbl}'")
        }
    }

    private suspend fun flushSnapshotBatch(
        txIndexer: TxIndexer,
        resumeSeq: Long,
        table: String,
        rows: List<Map<String, Any?>>,
    ) {
        val token = ProtoAny.pack(sqliteSourceToken {
            latestSeq = resumeSeq
            snapshotCompleted = false
        }, PROTO_TAG)

        txIndexer.indexTx(token) { openTx ->
            for (row in rows) writeRow(openTx, dbName, "public", table, "r", row, null)
            TxResult.Committed()
        }
    }

    override fun close() {
        LOG.info("[$dbName] Closing external source")
    }

    // --- helpers (pure JDBC, called from runInterruptible) ---

    private fun beginImmediate(conn: Connection) {
        conn.createStatement().use { it.execute("BEGIN IMMEDIATE") }
    }

    private fun ensureCdcTable(conn: Connection) {
        conn.createStatement().use { stmt ->
            stmt.execute(
                """
                CREATE TABLE IF NOT EXISTS "$cdcTable" (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT,
                    tbl TEXT NOT NULL,
                    op  TEXT NOT NULL,
                    data TEXT NOT NULL
                )""".trimIndent()
            )
        }
    }

    private fun assertCdcLogEmpty(conn: Connection) {
        conn.createStatement().use { stmt ->
            stmt.executeQuery("SELECT COUNT(*) FROM \"$cdcTable\"").use { rs ->
                rs.next()
                val count = rs.getLong(1)
                if (count > 0) {
                    throw Incorrect(
                        "CDC log '$cdcTable' already has $count unprocessed rows — refusing to start a fresh snapshot. " +
                                "Resume from a valid token, or DROP the CDC table and its triggers before re-attaching.",
                        errorCode = "xtdb.sqlite/cdc-log-not-empty",
                        data = mapOf("cdcTable" to cdcTable, "rowCount" to count),
                    )
                }
            }
        }
    }

    private fun readColumns(conn: Connection, table: String): List<String> {
        val cols = conn.createStatement().use { stmt ->
            stmt.executeQuery("PRAGMA table_info(\"$table\")").use { rs ->
                buildList {
                    while (rs.next()) add(rs.getString("name"))
                }
            }
        }
        if (cols.isEmpty()) {
            throw Incorrect(
                "table '$table' not found in SQLite database at '$path'",
                errorCode = "xtdb.sqlite/missing-table",
                data = mapOf("table" to table, "path" to path),
            )
        }
        return cols
    }

    private fun readMaxSeq(conn: Connection): Long =
        conn.createStatement().use { stmt ->
            stmt.executeQuery("SELECT COALESCE(MAX(seq), 0) FROM \"$cdcTable\"").use { rs ->
                rs.next(); rs.getLong(1)
            }
        }

    private fun readTableRows(conn: Connection, table: String, columns: List<String>): List<Map<String, Any?>> =
        conn.createStatement().use { stmt ->
            stmt.executeQuery("SELECT * FROM \"$table\"").use { rs ->
                buildList {
                    while (rs.next()) {
                        add(columns.associateWith { col -> rs.getObject(col) })
                    }
                }
            }
        }

    /**
     * On each poll, detect schema drift on the source tables and reinstall triggers so the CDC log
     * captures new columns. Still racy between an ALTER and the very next DML on the same connection
     * — by design, trigger-based CDC can't close that window. Callers doing `ALTER; DML;` back-to-back
     * should expect to wait one poll cycle for the new column to flow through.
     */
    private fun refreshTriggersIfChanged(conn: Connection, cachedCols: MutableMap<String, List<String>>) {
        for (table in tableIncludeList) {
            val current = readColumns(conn, table)
            if (cachedCols[table] != current) {
                LOG.info("[$dbName] Schema drift detected on '$table' — reinstalling triggers (${current.size} cols)")
                installTriggers(conn, table, current)
                cachedCols[table] = current
            }
        }
    }

    private fun installTriggers(conn: Connection, table: String, cols: List<String> = readColumns(conn, table)) {
        val newJson = cols.joinToString(", ") { "'$it', NEW.\"$it\"" }
        val oldJson = cols.joinToString(", ") { "'$it', OLD.\"$it\"" }

        conn.createStatement().use { stmt ->
            stmt.execute("DROP TRIGGER IF EXISTS \"__xtdb_cdc_${table}_ins\"")
            stmt.execute("DROP TRIGGER IF EXISTS \"__xtdb_cdc_${table}_upd\"")
            stmt.execute("DROP TRIGGER IF EXISTS \"__xtdb_cdc_${table}_del\"")

            stmt.execute(
                """
                CREATE TRIGGER "__xtdb_cdc_${table}_ins" AFTER INSERT ON "$table"
                FOR EACH ROW BEGIN
                    INSERT INTO "$cdcTable" (tbl, op, data) VALUES ('$table', 'c', json_object($newJson));
                END""".trimIndent()
            )
            stmt.execute(
                """
                CREATE TRIGGER "__xtdb_cdc_${table}_upd" AFTER UPDATE ON "$table"
                FOR EACH ROW BEGIN
                    INSERT INTO "$cdcTable" (tbl, op, data) VALUES ('$table', 'u', json_object($newJson));
                END""".trimIndent()
            )
            stmt.execute(
                """
                CREATE TRIGGER "__xtdb_cdc_${table}_del" AFTER DELETE ON "$table"
                FOR EACH ROW BEGIN
                    INSERT INTO "$cdcTable" (tbl, op, data) VALUES ('$table', 'd', json_object($oldJson));
                END""".trimIndent()
            )
        }
    }

    companion object {
        private val JSON = Json { ignoreUnknownKeys = true }

        internal fun parseJsonRow(json: String): Map<String, Any?> {
            val element = JSON.parseToJsonElement(json)
            require(element is JsonObject) { "Expected JSON object, got ${element::class.simpleName}" }
            return element.mapValues { (_, v) -> jsonToAny(v) }
        }

        private fun jsonToAny(el: JsonElement): Any? = when (el) {
            is JsonNull -> null
            is JsonPrimitive -> when {
                el.isString -> el.content
                el.booleanOrNull != null -> el.booleanOrNull
                el.longOrNull != null -> el.longOrNull
                else -> el.content.toDoubleOrNull() ?: el.content
            }
            else -> el.toString()
        }
    }
}

private fun writeRow(
    openTx: OpenTx,
    dbName: String,
    schema: String,
    table: String,
    op: String,
    after: Map<String, Any?>?,
    before: Map<String, Any?>?,
) {
    val openTxTable = openTx.table(TableRef(dbName, schema, table))

    when (op) {
        "c", "r", "u" -> {
            requireNotNull(after) { "Missing row data for $op operation" }
            val docMap = after.toMutableMap()

            val id = docMap["_id"] ?: throw Incorrect("Missing '_id' in row from $schema.$table")

            val explicitValidFrom = (docMap.remove("_valid_from") as? Instant)?.asMicros
            val explicitValidTo = (docMap.remove("_valid_to") as? Instant)?.asMicros

            if (explicitValidTo != null && explicitValidFrom == null)
                throw Incorrect("'_valid_to' requires '_valid_from'")

            openTxTable.logPut(
                ByteBuffer.wrap(id.asIid),
                explicitValidFrom ?: openTx.systemFrom,
                explicitValidTo ?: Long.MAX_VALUE,
            ) { openTxTable.docWriter.writeObject(docMap) }
        }

        "d" -> {
            requireNotNull(before) { "Missing 'before' data for delete" }
            val id = before["_id"] ?: throw Incorrect("Missing '_id' in 'before' for delete on $schema.$table")

            openTxTable.logDelete(
                ByteBuffer.wrap(id.asIid),
                openTx.systemFrom,
                Long.MAX_VALUE,
            )
        }

        else -> throw Incorrect("Unknown CDC op: '$op'")
    }
}
