package xtdb.kafka.cdc

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import xtdb.api.Xtdb
import xtdb.api.module.XtdbModule
import xtdb.util.logger
import java.nio.file.Path
import java.nio.file.Paths
import java.time.Duration

private val LOG = FileCdcFactory::class.logger

/**
 * Factory for creating file-based CDC modules
 */
@Serializable
@SerialName("!FileCdc")
data class FileCdcFactory(
    var outputDirectory: String = "cdc-output",
    var prettyPrint: Boolean = true,
    var enabled: Boolean = true,
    var lagThreshold: String = "PT10S"  // ISO-8601 duration format
) : XtdbModule.Factory {
    
    override val moduleKey: String = "file-cdc"
    
    override fun openModule(xtdb: Xtdb): XtdbModule {
        return if (enabled) {
            // LOG.info("Enabling file-based CDC output to directory: $outputDirectory")
            FileCdcModule(this, xtdb)
        } else {
            // LOG.info("File-based CDC output is disabled")
            NoOpFileCdcModule()
        }
    }
    
    fun buildConfig(): FileCdcConfig {
        return FileCdcConfig(
            outputDirectory = Paths.get(outputDirectory),
            prettyPrint = prettyPrint
        )
    }
    
    fun getLagThresholdDuration(): Duration {
        return Duration.parse(lagThreshold)
    }
}

/**
 * Configuration for file-based CDC
 */
data class FileCdcConfig(
    val outputDirectory: Path,
    val prettyPrint: Boolean = true
)

/**
 * Module that provides file-based CDC functionality
 */
class FileCdcModule(
    private val factory: FileCdcFactory,
    private val xtdb: Xtdb
) : XtdbModule {
    
    private var fileCdcWriter: FileCdcWriter? = null
    
    init {
        // Initialize the file CDC writer
        val config = factory.buildConfig()
        fileCdcWriter = FileCdcWriter(config.outputDirectory, config.prettyPrint)
        // LOG.info("File CDC writer initialized: output directory = ${config.outputDirectory}")
    }
    
    override fun close() {
        fileCdcWriter?.close()
    }
}

/**
 * No-op module when file CDC is disabled
 */
class NoOpFileCdcModule : XtdbModule {
    override fun close() {
        // Nothing to close
    }
}

/**
 * Registration class for the FileCdc module
 */
class FileCdcRegistration : XtdbModule.Registration {
    override fun register(registry: XtdbModule.Registry) {
        registry.registerModuleFactory(FileCdcFactory::class)
    }
}