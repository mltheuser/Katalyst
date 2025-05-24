package dsl.persistance

import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Mutex
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeoutException
import java.util.regex.PatternSyntaxException
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes

class InMemoryLockableKeyValueStore : LockableKeyValueStore {

    internal val cleanupInterval = 1.minutes

    private class CacheEntry(
        val value: String, expiresAfter: Duration
    ) {

        val expiresAtMillis: Long

        init {
            if (expiresAfter == Duration.INFINITE) {
                this.expiresAtMillis = Long.MAX_VALUE
            } else {
                this.expiresAtMillis = System.currentTimeMillis() + expiresAfter.inWholeMilliseconds
            }
        }
    }

    private val dataStore = ConcurrentHashMap<String, CacheEntry>()
    private val keyMutexes = ConcurrentHashMap<String, Mutex>()
    private val activeLocks = ConcurrentHashMap<String, String>() // Maps key -> lockHandle.key

    // Coroutine scope for background tasks like cleanup
    // SupervisorJob ensures that if one child coroutine fails, others are not affected.
    private val coroutineScope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    private val cleanupJob: Job

    init {
        cleanupJob = coroutineScope.launch {
            while (isActive) {
                delay(cleanupInterval.inWholeMilliseconds)
                cleanupExpiredEntries()
            }
        }
    }

    private fun cleanupExpiredEntries() {
        val now = System.currentTimeMillis()
        val keysToRemove = mutableListOf<String>()

        dataStore.entries.forEach { entry ->
            if (now > entry.value.expiresAtMillis) {
                keysToRemove.add(entry.key)
            }
        }

        keysToRemove.forEach { key ->
            removeEntry(key)
        }
    }

    private fun removeEntry(key: String) {
        dataStore.remove(key)
        activeLocks.remove(key) // Clean up lock metadata if key is removed
        keyMutexes.remove(key)  // Clean up mutex if key is removed
    }

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

        if (activeLock != lockHandle.id) {
            return Result.failure(IllegalStateException("Invalid lock handle for key '$key'. Provided: ${lockHandle.id}, Expected: $activeLock"))
        }

        activeLocks.remove(key)

        keyMutexes[key]?.unlock()

        return Result.success(Unit)
    }

    override suspend fun exists(key: String): Result<Boolean> {
        return Result.success(dataStore.containsKey(key))
    }

    override suspend fun get(key: String, lockHandle: LockHandle): Result<String> {
        return dataStore[key]?.let { Result.success(it.value) }
            ?: Result.failure(NoSuchElementException("Key '$key' not found in store."))
    }

    override suspend fun set(key: String, value: String, lockHandle: LockHandle, expiresAfter: Duration): Result<Unit> {
        dataStore[key] = CacheEntry(value, expiresAfter)
        return Result.success(Unit)
    }

    override suspend fun findKeysByPattern(pattern: String): Result<Iterator<String>> = try {
        // 1. Construct the regex pattern correctly.
        //    The user's `pattern` string uses '*' as a wildcard for 'any sequence of zero or more characters'.
        //    Other characters, including regex metacharacters (e.g., '.', '+', '?'), should be treated literally.

        //    Split the pattern by our wildcard character '*'.
        val parts = pattern.split('*')

        //    Escape each part to ensure any regex metacharacters within them are treated literally.
        //    For example, if pattern is "config*.ini", parts are "config" and ".ini".
        //    Regex.escape(".ini") becomes "\\.ini".
        val escapedParts = parts.map { Regex.escape(it) }

        //    Join the escaped parts with '.*' which is the regex equivalent of our wildcard.
        //    If pattern was "a*b*c":
        //    parts = ["a", "b", "c"]
        //    escapedParts = ["a", "b", "c"] (assuming a,b,c are not regex meta)
        //    regexPatternText = "a.*b.*c"
        //
        //    If pattern was "*key*":
        //    parts = ["", "key", ""]
        //    escapedParts = ["", "key", ""]
        //    regexPatternText = ".*key.*"
        //
        //    If pattern was "file.name*":
        //    parts = ["file.name", ""]
        //    escapedParts = ["file\\.name", ""] (Regex.escape("file.name") -> "file\\.name")
        //    regexPatternText = "file\\.name.*"
        val regexPatternText = escapedParts.joinToString(separator = ".*")

        //    Anchor the pattern to match the whole key string.
        val regex = Regex("^$regexPatternText$") // Ensure it matches the entire key

        // 2. Iterate over current keys and filter.
        //    dataStore.keys provides a KeySetView. Iterating it is weakly consistent.
        //    Filtering and then collecting to a list provides a snapshot of matching keys.
        val matchingKeys = dataStore.keys.asSequence().filter { key ->
            val entry = dataStore[key] // Check for concurrent removal
            val isMatch = regex.matches(key)
            entry != null && isMatch
        }

        // 3. Return an iterator over the collected matching keys.
        Result.success(matchingKeys.iterator())
    } catch (e: PatternSyntaxException) {
        // This can be thrown by Regex(pattern) if the constructed pattern is somehow invalid,
        // though our construction method should be safe.
        Result.failure(IllegalArgumentException("Invalid pattern syntax generated from '$pattern'. Error: ${e.message}", e))
    } catch (e: Exception) { // Catch any other unexpected errors during the process
        Result.failure(RuntimeException("Failed to find keys by pattern '$pattern'. Error: ${e.message}", e))
    }

    override suspend fun delete(key: String, lockHandle: LockHandle): Result<Unit> {
        removeEntry(key)
        return Result.success(Unit)
    }
}