package dsl.persistance

import dsl.components.Instance
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.withTimeoutOrNull
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeoutException
import kotlin.time.Duration

class InMemoryLockableKeyValueStore : LockableKeyValueStore {

    private val dataStore = ConcurrentHashMap<String, Instance>()
    private val keyMutexes = ConcurrentHashMap<String, Mutex>()
    private val activeLocks = ConcurrentHashMap<String, String>() // Maps key -> lockHandle.key

    override suspend fun lock(key: String, timeout: Duration): Result<LockHandle> {
        val mutex = keyMutexes.computeIfAbsent(key) { Mutex() }

        val acquiredSuccessfully = withTimeoutOrNull(timeout.inWholeMilliseconds) {
            mutex.lock()
            true
        } ?: false       // Timeout occurred or lock() was interrupted

        return if (acquiredSuccessfully) {
            val keyForLock = UUID.randomUUID().toString()
            activeLocks[key] = keyForLock

            Result.success(LockHandle(keyForLock))
        } else {
            Result.failure(TimeoutException("Timeout ($timeout) trying to acquire lock for key '$key'."))
        }
    }

    override suspend fun unlock(key: String, lockHandle: LockHandle): Result<Unit> {
        val activeLock = activeLocks[key]
        if (activeLock == null) {
            return Result.success(Unit)
        }

        if (activeLock != lockHandle.key) {
            return Result.failure(IllegalStateException("Invalid lock handle for key '$key'. Provided: ${lockHandle.key}, Expected: $activeLock"))
        }

        activeLocks.remove(key)

        keyMutexes[key]?.unlock()

        return Result.success(Unit)
    }

    override suspend fun exists(key: String): Result<Boolean> {
        return Result.success(dataStore.containsKey(key))
    }

    override suspend fun get(key: String, lockHandle: LockHandle): Result<Instance> {
        val currentActiveLockOnKey = activeLocks[key]

        if (currentActiveLockOnKey == null) {
            return Result.failure(
                IllegalStateException(
                    "No active lock found for key '$key'. GET operation denied. The lock may have been released or never acquired."
                )
            )
        }
        if (currentActiveLockOnKey != lockHandle.key) {
            return Result.failure(IllegalStateException("Invalid lock handle for key '$key'. Provided: '${lockHandle.key}', Current active lock: '$currentActiveLockOnKey'. GET operation denied."))
        }

        return dataStore[key]?.let { Result.success(it) }
            ?: Result.failure(NoSuchElementException("Key '$key' not found in store."))
    }

    override suspend fun set(key: String, value: Instance, lockHandle: LockHandle): Result<Unit> {
        val currentActiveLockOnKey = activeLocks[key]

        if (currentActiveLockOnKey == null) {
            return Result.failure(
                IllegalStateException(
                    "No active lock found for key '$key'. SET operation denied. The lock may have been released or never acquired."
                )
            )
        }
        if (currentActiveLockOnKey != lockHandle.key) {
            return Result.failure(IllegalStateException("Invalid lock handle for key '$key'. Provided: '${lockHandle.key}', Current active lock: '$currentActiveLockOnKey'. SET operation denied."))
        }

        dataStore[key] = value
        return Result.success(Unit)
    }

    override suspend fun delete(key: String, lockHandle: LockHandle): Result<Unit> {
        val currentActiveLockOnKey = activeLocks[key]

        if (currentActiveLockOnKey == null) {
            return Result.failure(
                IllegalStateException(
                    "No active lock found for key '$key'. DELETE operation denied. The lock may have been released or never acquired."
                )
            )
        }

        if (currentActiveLockOnKey != lockHandle.key) {
            return Result.failure(
                IllegalStateException(
                    "Invalid lock handle for key '$key'. Provided: '${lockHandle.key}', Current active lock: '$currentActiveLockOnKey'. DELETE operation denied."
                )
            )
        }

        dataStore.remove(key)
        activeLocks.remove(key)
        keyMutexes.remove(key)
        return Result.success(Unit)
    }
}