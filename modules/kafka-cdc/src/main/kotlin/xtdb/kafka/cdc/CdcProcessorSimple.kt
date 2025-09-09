package xtdb.kafka.cdc

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import org.apache.arrow.memory.BufferAllocator
import xtdb.api.log.Log
import xtdb.api.log.MessageId
import xtdb.database.Database
import xtdb.util.logger
import xtdb.util.MsgIdUtil.offsetToMsgId
import java.time.Duration
import java.time.Instant
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import kotlin.coroutines.cancellation.CancellationException

private val LOG = CdcProcessorSimple::class.logger

/**
 * Simplified CDC processor for compilation testing
 */
class CdcProcessorSimple(
    private val db: Database,
    private val xtdb: xtdb.api.Xtdb,
    private val cdcProducer: CdcProducerInterface,
    private val allocator: BufferAllocator,
    private val meterRegistry: MeterRegistry,
    private val cdcConfig: CdcConfig,
    private val lagThreshold: Duration = Duration.ofSeconds(10)
) : Log.Subscriber, AutoCloseable {
    
    private val log = db.log
    private val epoch = log.epoch
    
    // Metrics
    private val processedEvents = Counter.builder("cdc.events.processed")
        .description("Number of CDC events processed")
        .register(meterRegistry)
    
    private val failedEvents = Counter.builder("cdc.events.failed")
        .description("Number of CDC events that failed to send")
        .register(meterRegistry)
    
    private val lagMs = AtomicLong(0)
    
    init {
        Gauge.builder("cdc.lag.ms", lagMs) { it.get().toDouble() }
            .description("CDC processing lag in milliseconds")
            .register(meterRegistry)
    }
    
    @Volatile
    override var latestProcessedMsgId: MessageId = offsetToMsgId(epoch, -1)
        private set
    
    override val latestSubmittedMsgId: MessageId
        get() = offsetToMsgId(epoch, log.latestSubmittedOffset)
    
    private val running = AtomicBoolean(true)
    
    override fun processRecords(records: List<Log.Record>) {
        records.forEach { record ->
            val msgId = offsetToMsgId(epoch, record.logOffset)
            
            try {
                // Simplified processing - just track that we processed something
                processedEvents.increment()
                latestProcessedMsgId = msgId
                
                // For now, just create a simple CDC event for any transaction
                val event = CdcEvent(
                    database = db.name,
                    tableName = "test_table",
                    recordId = "test_record",
                    operation = CdcOperation.CREATE,
                    beforeValue = null,
                    afterValue = """{"_id":"test_record","data":"test"}""",
                    timestamp = record.logTimestamp,
                    systemTime = record.logTimestamp,
                    txId = msgId.toString().toLongOrNull() ?: 0L,
                    transactionInfo = TransactionInfo(
                        id = msgId.toString(),
                        totalOrder = msgId.toString().toLongOrNull() ?: 0L,
                        dataCollectionOrder = 0L
                    )
                )
                
                // Send the event (this will be async)
                runBlocking {
                    cdcProducer.sendCdcEvent(event)
                }
                
            } catch (e: Exception) {
                // LOG.error("Error processing CDC for message $msgId", e)
                failedEvents.increment()
            }
        }
    }
    
    override fun close() {
        running.set(false)
    }
}