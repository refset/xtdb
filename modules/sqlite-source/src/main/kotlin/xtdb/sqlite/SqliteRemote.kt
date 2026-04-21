package xtdb.sqlite

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.modules.PolymorphicModuleBuilder
import kotlinx.serialization.modules.subclass
import xtdb.api.Remote

class SqliteRemote(
    val path: String,
) : Remote {

    override fun close() = Unit

    @Serializable
    @SerialName("!Sqlite")
    data class Factory(
        val path: String,
    ) : Remote.Factory<SqliteRemote> {
        override fun open() = SqliteRemote(path)
    }

    class Registration : Remote.Registration {
        override fun registerSerde(builder: PolymorphicModuleBuilder<Remote.Factory<*>>) {
            builder.subclass(Factory::class)
        }
    }
}
