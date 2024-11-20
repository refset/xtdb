package xtdb.cache

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.nio.ByteBuffer
import java.nio.file.Path
import java.util.concurrent.CompletableFuture.completedFuture
import kotlin.io.path.pathString

class MemoryCacheTest {

    private lateinit var allocator: BufferAllocator

    @BeforeEach
    fun setUp() {
        allocator = RootAllocator()
    }

    @AfterEach
    fun tearDown() {
        allocator.close()
    }

    inner class PathLoader : MemoryCache.PathLoader {
        private var idx = 0
        override fun load(path: Path): ByteBuffer =
            ByteBuffer.allocateDirect(path.last().pathString.toInt())
                .also { it.put(0, (++idx).toByte()) }
    }

    @Test
    fun `test memory cache`() {
        // just a starter-for-ten here, intent is that we go further down the property/deterministic testing route
        // significantly exercised E2E by the rest of the test-suite and benchmarks.

        MemoryCache(allocator, 250, PathLoader()).use { cache ->

            var t1Evicted = false

            assertAll("get t1", {
                val onEvict = AutoCloseable { t1Evicted = true }

                cache.get(Path.of("t1/100")) { completedFuture(it to onEvict) }.use { b1 ->
                    assertEquals(1, b1.getByte(0))

                    assertEquals(Stats(100, 0, 150), cache.stats)
                }

                cache.get(Path.of("t1/100")) { completedFuture(it to onEvict) }.use { b1 ->
                    assertEquals(1, b1.getByte(0))
                }

                Thread.sleep(50)
                assertEquals(Stats(0, 100, 150), cache.stats)
                assertFalse(t1Evicted)
            })

            var t2Evicted = false

            assertAll("t2", {
                val onEvict = AutoCloseable { t2Evicted = true }

                cache.get(Path.of("t2/50")) { completedFuture(it to onEvict) }.use { b1 ->
                    assertEquals(2, b1.getByte(0))

                    assertEquals(Stats(50, 100, 100), cache.stats)
                }

                Thread.sleep(100)
                assertEquals(Stats(0, 150, 100), cache.stats)
            })

            assertFalse(t1Evicted)
            assertFalse(t2Evicted)

            assertAll("t3 evicts t2/t1", {
                cache.get(Path.of("t3/170")) { completedFuture(it to null) }.use { b1 ->
                    assertEquals(3, b1.getByte(0))
                    assertTrue(t1Evicted)

                    // definitely needs to evict t1, may or may not evict t2
                    val stats = cache.stats
                    assertEquals(170, stats.pinnedBytes)
                    assertEquals(80, stats.evictableBytes + stats.freeBytes)
                }

                Thread.sleep(100)
                val stats = cache.stats
                assertEquals(0, stats.pinnedBytes)
                assertEquals(250, stats.evictableBytes + stats.freeBytes)
            })
        }
    }
}