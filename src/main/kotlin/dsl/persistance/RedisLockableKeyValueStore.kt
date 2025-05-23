package dsl.persistance

import kotlinx.coroutines.future.asDeferred
import org.redisson.Redisson
import org.redisson.api.RPermitExpirableSemaphore
import org.redisson.api.RedissonClient
import org.redisson.config.Config
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import kotlin.time.Duration

class RedisLockableKeyValueStore(
    private val config: PersistenceConfig.Redis // Store config to access its properties later
) : LockableKeyValueStore {

    private val redisson: RedissonClient

    companion object {
        private const val LOCK_NAMESPACE_PREFIX = "dslock:" // Namespace for lock keys
        private const val DATA_NAMESPACE_PREFIX = "dsdata:" // Namespace for data keys
    }

    init {
        val redissonCfg = Config()
        val singleServerConfig = redissonCfg.useSingleServer()
            .setAddress("redis://${config.host}:${config.port}")
            .setDatabase(config.database)
            .setConnectTimeout(config.connectionTimeoutMillis.toInt())
            .setTimeout(config.commandTimeoutMillis.toInt())

        config.password?.let { singleServerConfig.setPassword(it) }

        this.redisson = Redisson.create(redissonCfg)
    }

    private fun getInternalLockName(key: String) = "$LOCK_NAMESPACE_PREFIX$key"
    private fun getInternalDataKey(key: String) = "$DATA_NAMESPACE_PREFIX$key"

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
                // Successfully acquired a permit. Return LockHandle containing the permit ID.
                Result.success(LockHandle(permitId))
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
                    "Error acquiring lock for key '$key' (lock name: $internalLockName): ${e.message}",
                    e
                )
            )
        }
    }

    override suspend fun unlock(key: String, lockHandle: LockHandle): Result<Unit> {
        val internalLockName = getInternalLockName(key)
        val semaphore: RPermitExpirableSemaphore = redisson.getPermitExpirableSemaphore(internalLockName)
        val permitId = lockHandle.id // The permit ID obtained from a successful lock() call.

        return try {
            semaphore.releaseAsync(permitId).asDeferred().await()
            return Result.success(Unit)
        } catch (e: Exception) {
            Result.failure(
                RuntimeException(
                    "Error releasing lock for key '$key' (lock name: $internalLockName): ${e.message}",
                    e
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
                    "Error checking existence for key '$key' (data key: $internalDataKey): ${e.message}",
                    e
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

    override suspend fun set(key: String, value: String, lockHandle: LockHandle): Result<Unit> {
        val internalDataKey = getInternalDataKey(key)
        // Note: Assumes valid lockHandle.
        val bucket = redisson.getBucket<String>(internalDataKey)
        return try {
            bucket.setAsync(value).await() // Use async version
            Result.success(Unit)
        } catch (e: Exception) {
            Result.failure(RuntimeException("Error setting key '$key' (data key: $internalDataKey): ${e.message}", e))
        }
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
