package xtdb.kafka.cdc

import java.util.concurrent.CompletableFuture

/**
 * Interface for CDC producers - allows for both Kafka and file-based implementations
 */
interface CdcProducerInterface : AutoCloseable {
    suspend fun sendCdcEvent(event: CdcEvent): CompletableFuture<Unit>
}