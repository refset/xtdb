package xtdb.kafka.cdc

import io.micrometer.core.instrument.MeterRegistry
import org.apache.arrow.memory.BufferAllocator
import xtdb.api.Xtdb
import xtdb.api.module.XtdbModule
import xtdb.database.Database
import xtdb.util.logger
// import xtdb.util.info
import java.time.Duration
import java.util.Properties

private val LOG = CdcFactory::class.logger

/**
 * Factory for creating CDC-enabled components
 */
class CdcFactory : XtdbModule.Factory {
    
    override val moduleKey: String = "kafka-cdc"
    
    var bootstrapServers: String = "localhost:9092"
    var schemaRegistryUrl: String = "http://localhost:8081"
    var topicPrefix: String = "xtdb.cdc."
    var nodeName: String? = null
    var enabled: Boolean = true
    var useJsonStringApproach: Boolean = true
    var lagThreshold: Duration = Duration.ofSeconds(10)
    var producerProperties: Properties? = null
    
    override fun openModule(xtdb: Xtdb): XtdbModule {
        return if (enabled) {
            // LOG.info("Enabling CDC output to Kafka at $bootstrapServers with schema registry at $schemaRegistryUrl")
            CdcModule(this, xtdb)
        } else {
            // LOG.info("CDC output is disabled")
            NoOpCdcModule()
        }
    }
    
    fun buildConfig(xtdb: Xtdb): CdcConfig {
        return CdcConfig(
            bootstrapServers = bootstrapServers,
            schemaRegistryUrl = schemaRegistryUrl,
            topicPrefix = topicPrefix,
            nodeName = nodeName ?: "xtdb-node",
            useJsonStringApproach = useJsonStringApproach,
            producerProperties = producerProperties?.toMap() as? Map<String, Any>
        )
    }
}

/**
 * Module that provides CDC functionality
 */
class CdcModule(
    private val factory: CdcFactory,
    private val xtdb: Xtdb
) : XtdbModule {
    
    private var cdcProcessor: CdcProcessorSimple? = null
    
    fun startCdcProcessor(
        db: Database,
        allocator: BufferAllocator,
        meterRegistry: MeterRegistry
    ) {
        if (cdcProcessor == null) {
            val cdcProducer = CdcProducer(factory.buildConfig(xtdb))
            cdcProcessor = CdcProcessorSimple(
                db = db,
                xtdb = xtdb,
                cdcProducer = cdcProducer,
                allocator = allocator,
                meterRegistry = meterRegistry,
                cdcConfig = factory.buildConfig(xtdb),
                lagThreshold = factory.lagThreshold
            )
            // LOG.info("Started CDC processor for database ${db.name}")
        }
    }
    
    override fun close() {
        cdcProcessor?.close()
    }
}

/**
 * No-op module when CDC is disabled
 */
class NoOpCdcModule : XtdbModule {
    override fun close() {
        // Nothing to close
    }
}