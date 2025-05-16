package dsl.persistance

import dsl.components.Instance
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock


class InstanceCacheEntry(
    val instance: Instance,
    val lockHandle: LockHandle,
    val ownerCache: InstanceCache,
) {
    @Volatile var refCount: Int = 0
        private set

    private val entryLock = ReentrantLock()

    fun increaseRefCountByOne() {
        entryLock.withLock {
            if (refCount < 0) { // Should not happen in correct operation
                throw IllegalStateException("Cannot increase refCount for an entry with negative refCount (id: ${instance.instanceId}).")
            }
            refCount++
        }
    }

    fun decreaseRefCountByOne() {
        var shouldFinalizeRemoval = false
        lateinit var instanceToFinalize: Instance // Use the instance from this entry

        entryLock.withLock {
            if (refCount <= 0) {
                // This might happen if close() is called redundantly after the entry is already processed for removal.
                // The AtomicBoolean in InstanceReference should prevent multiple calls to decreaseRefCountByOne per reference.
                // If refCount is already 0, this entry is already on its way out or gone.
                println("Warning: decreaseRefCountByOne called on entry for ${instance.instanceId} with refCount $refCount.")
                return
            }

            refCount--

            if (refCount == 0) {
                // Attempt to remove THIS entry from the cache.
                if (ownerCache.atomicallyRemoveEntryFromCache(instance.instanceId, this)) {
                    shouldFinalizeRemoval = true
                    instanceToFinalize = this.instance
                } else {
                    // Entry was not removed by us. Could be:
                    // 1. Already removed by another thread/process.
                    // 2. Replaced in the cache by a different entry for the same ID (less likely with current design but possible if cache had explicit overwrite).
                    // In this case, this specific entry instance should not trigger DB persistence.
                    println("Warning:  Entry for ${instance.instanceId} was not in cache or was a different entry object when trying to remove for finalization.")
                }
            }
        }

        if (shouldFinalizeRemoval) {
            ownerCache.coroutineScope.launch {
                ownerCache.finalizeAndUnlockInstanceInDb(instanceToFinalize, lockHandle)
            }
        }
    }
}

class InstanceReference(
    val instance: Instance, private val entry: InstanceCacheEntry
) : AutoCloseable {

    private val closed = AtomicBoolean(false)

    init {
        entry.increaseRefCountByOne()
    }

    override fun close() {
        if (closed.compareAndSet(false, true)) {
            entry.decreaseRefCountByOne()
        }
    }
}

class InstanceCache(
    internal val coroutineScope: CoroutineScope
) {
    private val cache = ConcurrentHashMap<String, InstanceCacheEntry>()

    suspend fun register(newInstance: Instance): Result<Unit> {
        val id = newInstance.instanceId

        if (Persistence.store.exists(id).getOrThrow()) {
            throw IllegalStateException("Instance $id already exists in persistence. Cannot register.")
        }

        val lockHandle = Persistence.store.lock(id).getOrThrow()
        Persistence.store.set(id, newInstance, lockHandle)
        Persistence.store.unlock(id, lockHandle).getOrThrow()

        return Result.success(Unit)
    }

    suspend fun lookup(id: String): Result<InstanceReference> {
        cache.get(id)?.let { entry ->
            return Result.success(InstanceReference(entry.instance, entry))
        }

        val idExists = Persistence.store.exists(id).getOrThrow()
        if (idExists) {
            val lockHandle = Persistence.store.lock(id).getOrThrow()
            val instance = Persistence.store.get(id, lockHandle).getOrThrow()

            val newCacheEntry = InstanceCacheEntry(instance, lockHandle, this)
            cache[id] = newCacheEntry

            return Result.success(InstanceReference(instance, newCacheEntry))
        } else {
            return Result.failure(IllegalStateException("Instance $id was not found."))
        }

    }

    internal fun atomicallyRemoveEntryFromCache(id: String, entry: InstanceCacheEntry): Boolean {
        val removed = cache.remove(id, entry)
        return removed
    }

    internal suspend fun finalizeAndUnlockInstanceInDb(instance: Instance, lockHandle: LockHandle) {
        instance.awaitIdle()
        Persistence.store.set(instance.instanceId, instance, lockHandle)
        Persistence.store.unlock(instance.instanceId, lockHandle)
    }
}

class InstanceHandle(
    private val instanceId: String
) {
    /**
     * Provides a scope function `instance { ... }` syntax.
     * Ensures the code block executes within the context of this specific Instance.
     */
    suspend operator fun invoke(block: suspend Instance.() -> Unit) {
        withInstance(instanceId) { instanceReference ->
            instanceReference.use { ref ->
                ref.instance {
                    block()
                }
            }
        }
    }
}

suspend fun withInstance(instanceId: String, block: suspend (instanceReference: InstanceReference) -> Unit) {
    val instanceReferenceResult = Instance.instanceCache.lookup(instanceId)
    val instanceReference = instanceReferenceResult.getOrThrow()
    return block(instanceReference)
}