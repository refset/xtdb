package xtdb.sqlite

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import xtdb.api.nodeConfig
import xtdb.database.Database

class SqliteSourceFactoryTest {

    private fun protoRoundTrip(factory: SqliteSource.Factory): SqliteSource.Factory {
        val dbConfig = Database.Config().externalSource(factory)
        val restored = Database.Config.fromProto(dbConfig.serializedConfig)
        return restored.externalSource as SqliteSource.Factory
    }

    @Test
    fun `proto round-trips factory`() {
        val original = SqliteSource.Factory(
            remote = "sqlite",
            cdcTable = "my_cdc",
            tableIncludeList = listOf("users", "orders"),
        )

        val restored = protoRoundTrip(original)

        assertEquals("sqlite", restored.remote)
        assertEquals("my_cdc", restored.cdcTable)
        assertEquals(listOf("users", "orders"), restored.tableIncludeList)
    }

    @Test
    fun `YAML round-trips factory as external source`() {
        val yaml = """
            externalSource: !Sqlite
              remote: sqlite
              cdcTable: my_cdc
              tableIncludeList: [users]
        """.trimIndent()

        val config = Database.Config.fromYaml(yaml)
        val factory = config.externalSource as SqliteSource.Factory

        assertEquals("sqlite", factory.remote)
        assertEquals("my_cdc", factory.cdcTable)
        assertEquals(listOf("users"), factory.tableIncludeList)
    }

    @Test
    fun `cdcTable defaults to __xtdb_cdc_log__ when omitted in YAML`() {
        val yaml = """
            externalSource: !Sqlite
              remote: sqlite
              tableIncludeList: [users]
        """.trimIndent()

        val factory = Database.Config.fromYaml(yaml).externalSource as SqliteSource.Factory

        assertEquals("__xtdb_cdc_log__", factory.cdcTable)
    }

    @Test
    fun `node config decodes Sqlite remote under remotes`() {
        val yaml = """
            remotes:
              sqlite: !Sqlite
                path: /var/lib/xtdb/app.db
        """.trimIndent()

        val config = nodeConfig(yaml)
        val remote = config.remotes["sqlite"] as SqliteRemote.Factory

        assertEquals("/var/lib/xtdb/app.db", remote.path)
    }

    @Test
    fun `dual !Sqlite tag is disambiguated by YAML position`() {
        // Both SqliteSource.Factory (ExternalSource) and SqliteRemote.Factory (Remote)
        // carry @SerialName("!Sqlite"). Kaml must dispatch by polymorphic root, not by tag.
        val nodeYaml = """
            remotes:
              sqlite: !Sqlite
                path: /tmp/a.db
        """.trimIndent()

        val dbYaml = """
            externalSource: !Sqlite
              remote: sqlite
              tableIncludeList: [t]
        """.trimIndent()

        val nodeRemote = nodeConfig(nodeYaml).remotes["sqlite"]
        val extSource = Database.Config.fromYaml(dbYaml).externalSource

        assertTrue(nodeRemote is SqliteRemote.Factory, "node remote should resolve to SqliteRemote.Factory")
        assertTrue(extSource is SqliteSource.Factory, "external source should resolve to SqliteSource.Factory")
    }
}
