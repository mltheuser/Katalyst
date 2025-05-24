package dsl.persistance

import kotlinx.coroutines.future.asDeferred
import org.redisson.Redisson
import org.redisson.api.RPermitExpirableSemaphore
import org.redisson.api.RedissonClient
import org.redisson.api.options.KeysScanOptions
import org.redisson.config.Config
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import kotlin.time.Duration


class RedisLockableKeyValueStore(
    private val config: PersistenceConfig.Redis // Store config to access its properties later
) : LockableKeyValueStore {

    private val redisson: RedissonClient

    // Tracks active locks: internalLockName -> permitId
    // Using ConcurrentHashMap for thread-safety as lock/unlock can be called concurrently
    private val activeLocks = ConcurrentHashMap<String, String>()

    companion object {
        private const val LOCK_NAMESPACE_PREFIX = "dslock:" // Namespace for lock keys
        private const val DATA_NAMESPACE_PREFIX = "dsdata:" // Namespace for data keys
    }

    init {
        val redissonCfg = Config()
        val singleServerConfig = redissonCfg.useSingleServer().setAddress("redis://${config.host}:${config.port}")
            .setDatabase(config.database).setConnectTimeout(config.connectionTimeoutMillis.toInt())
            .setTimeout(config.commandTimeoutMillis.toInt())

        config.password?.let { singleServerConfig.setPassword(it) }

        this.redisson = Redisson.create(redissonCfg)

        // Register a shutdown hook to release locks and shutdown Redisson
        Runtime.getRuntime().addShutdownHook(Thread {
            println("RedisLockableKeyValueStore: Shutdown hook triggered. Releasing active locks...")
            // Use a set of keys to avoid ConcurrentModificationException if iterating and modifying
            val lockKeysToRelease = activeLocks.keys.toSet()
            lockKeysToRelease.forEach { internalLockName ->
                activeLocks[internalLockName]?.let { permitId ->
                    try {
                        val semaphore: RPermitExpirableSemaphore =
                            redisson.getPermitExpirableSemaphore(internalLockName)
                        // Use blocking release in shutdown hook as it's not a coroutine context
                        semaphore.release(permitId)
                    } catch (e: Exception) {
                        // Log error, but continue trying to release other locks and shutdown Redisson
                        System.err.println("RedisLockableKeyValueStore: Error releasing lock '$internalLockName' during shutdown: ${e.message}")
                    }
                }
            }
            activeLocks.clear() // Clear the map after attempting release

            println("RedisLockableKeyValueStore: Shutting down Redisson client...")
            if (!redisson.isShutdown && !redisson.isShuttingDown) {
                redisson.shutdown()
                println("RedisLockableKeyValueStore: Redisson client shut down.")
            }
        })
    }

    private fun getInternalLockName(key: String) = "$LOCK_NAMESPACE_PREFIX$key"
    private fun getInternalDataKey(key: String) = "$DATA_NAMESPACE_PREFIX$key"
    private fun getInternalDataKeyReversal(internalKey: String) = internalKey.removePrefix(DATA_NAMESPACE_PREFIX)

    override suspend fun lock(key: String, timeout: Duration): Result<LockHandle> {
        val internalLockName = getInternalLockName(key)
        val semaphore: RPermitExpirableSemaphore = redisson.getPermitExpirableSemaphore(internalLockName)

        return try {
            // 1. Initialize the semaphore with 1 permit if it's not already set.
            // RPermitExpirableSemaphore.trySetPermits ensures it's set only once.
            // This makes the semaphore behave like a mutex for this key.
            // We await its completion to ensure initialization before acquiring.
            semaphore.trySetPermitsAsync(1).asDeferred().await()
            // The boolean result of trySetPermitsAsync (true if set now, false if already set)
            // isn't critical here, as long as it's initialized to 1.

            // 2. Try to acquire a permit.
            //    waitTime: The 'timeout' parameter for this lock attempt.
            //    leaseTime: The duration for which the permit (lock) is valid after acquisition.
            //               We use config.lockWatchdogTimeoutMillis for this purpose.
            val permitId = semaphore.tryAcquireAsync(
                timeout.inWholeMilliseconds,        // waitTime to acquire the permit
                config.lockLeaseTimeMillis,   // leaseTime for the permit once acquired
                TimeUnit.MILLISECONDS
            ).asDeferred().await()

            if (permitId != null) {
                // Successfully acquired a permit.
                activeLocks[internalLockName] = permitId // Track the acquired lock
                Result.success(LockHandle(permitId)) //  Return LockHandle containing the permit ID.
            } else {
                // Timeout occurred before a permit could be acquired.
                Result.failure(
                    TimeoutException("Timeout acquiring lock for key '$key' (lock name: $internalLockName) after $timeout. No permit available within the specified time.")
                )
            }
        } catch (e: Exception) {
            // Handle exceptions from Redisson (e.g., connection issues) or other unexpected errors.
            Result.failure(
                RuntimeException(
                    "Error acquiring lock for key '$key' (lock name: $internalLockName): ${e.message}", e
                )
            )
        }
    }

    override suspend fun unlock(key: String, lockHandle: LockHandle): Result<Unit> {
        val internalLockName = getInternalLockName(key)
        val semaphore: RPermitExpirableSemaphore = redisson.getPermitExpirableSemaphore(internalLockName)
        val permitId = lockHandle.id // The permit ID obtained from a successful lock() call.

        activeLocks.remove(internalLockName, permitId) // Remove no matter the outcome of release with redis

        return try {
            semaphore.releaseAsync(permitId).asDeferred().await()
            return Result.success(Unit)
        } catch (e: Exception) {
            Result.failure(
                RuntimeException(
                    "Error releasing lock for key '$key' (lock name: $internalLockName): ${e.message}", e
                )
            )
        }
    }

    override suspend fun exists(key: String): Result<Boolean> {
        val internalDataKey = getInternalDataKey(key)
        return try {
            // Switched to getRBucket for clarity, though getBucket works too.
            val exists = redisson.getBucket<Any>(internalDataKey).isExistsAsync.asDeferred().await()
            Result.success(exists)
        } catch (e: Exception) {
            Result.failure(
                RuntimeException(
                    "Error checking existence for key '$key' (data key: $internalDataKey): ${e.message}", e
                )
            )
        }
    }

    override suspend fun get(key: String, lockHandle: LockHandle): Result<String> {
        val internalDataKey = getInternalDataKey(key)
        // Note: This method assumes the caller holds a valid lock via lockHandle.
        // It does not re-verify the lockHandle's permit against the live semaphore.
        val bucket = redisson.getBucket<String>(internalDataKey)
        return try {
            val value = bucket.getAsync().asDeferred().await() // Use async version
            if (value != null) {
                Result.success(value)
            } else {
                Result.failure(NoSuchElementException("Key '$key' not found (data key: $internalDataKey)."))
            }
        } catch (e: Exception) {
            Result.failure(RuntimeException("Error getting key '$key' (data key: $internalDataKey): ${e.message}", e))
        }
    }

    override suspend fun set(key: String, value: String, lockHandle: LockHandle, timeToLife: Duration): Result<Unit> {
        val internalDataKey = getInternalDataKey(key)
        // Note: Assumes valid lockHandle.
        val bucket = redisson.getBucket<String>(internalDataKey)
        return try {
            if (timeToLife.isInfinite()) {
                bucket.setAsync(value).asDeferred().await()
            } else {
                bucket.setAsync(value, timeToLife.inWholeMilliseconds, TimeUnit.MILLISECONDS).asDeferred().await()
            }
            Result.success(Unit)
        } catch (e: Exception) {
            Result.failure(RuntimeException("Error setting key '$key' (data key: $internalDataKey): ${e.message}", e))
        }
    }

    override suspend fun findKeysByPattern(pattern: String): Result<Iterator<String>> {
        val internalPattern = getInternalDataKey(pattern)
        val internalKeyIterator = redisson.keys.getKeys(KeysScanOptions.defaults().pattern(internalPattern))

        val keyIterator = internalKeyIterator.map { internalKey -> getInternalDataKeyReversal(internalKey) }

        return Result.success(keyIterator.iterator())
    }

    override suspend fun delete(key: String, lockHandle: LockHandle): Result<Unit> {
        val internalDataKey = getInternalDataKey(key)
        // Note: Assumes valid lockHandle.
        val bucket = redisson.getBucket<Any>(internalDataKey)
        return try {
            bucket.deleteAsync().asDeferred().await() // Use async version
            Result.success(Unit)
        } catch (e: Exception) {
            Result.failure(RuntimeException("Error deleting key '$key' (data key: $internalDataKey): ${e.message}", e))
        }
    }
}
