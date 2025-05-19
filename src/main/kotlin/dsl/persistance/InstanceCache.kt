package dsl.persistance

import dsl.components.Instance
import dsl.components.InteractionRegistry
import dsl.components.getFullPropertyName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.serializer
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.reflect.KProperty
import kotlin.reflect.KType


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
        pushInstance(id, newInstance, lockHandle)
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

            val instance = loadInstance(id, lockHandle)

            val newCacheEntry = InstanceCacheEntry(instance, lockHandle, this)
            cache[id] = newCacheEntry

            return Result.success(InstanceReference(instance, newCacheEntry))
        } else {
            return Result.failure(IllegalStateException("Instance $id was not found."))
        }

    }

    private suspend fun loadInstance(instanceId: String, lockHandle: LockHandle): Instance {
        println("[$instanceId] Loaded from persistence.")
        val jsonString = Persistence.store.get(instanceId, lockHandle).getOrThrow()
        val instanceRecord = InstanceRecord.fromString(jsonString)

        val interactions = instanceRecord.interactionRecords.map {
            val interaction = InteractionRegistry.getByName(it.interactionId)!!
            interaction.dependencies = interaction.dependencies.union(it.dependencies) as MutableSet<String>
            interaction.targets = interaction.targets.union(it.targets).toMutableSet()
            interaction
        }.toSet()

        val instance = Instance(instanceId, interactions)
        instance.initialRunPerformed = instanceRecord.initialRunPerformed
        instance.instanceState.putAll(
            instanceRecord.instanceState.mapValues { ReadOnlyEncodingProxy.fromJson(it.value) })

        return instance
    }

    internal fun atomicallyRemoveEntryFromCache(id: String, entry: InstanceCacheEntry): Boolean {
        val removed = cache.remove(id, entry)
        return removed
    }

    internal suspend fun finalizeAndUnlockInstanceInDb(instance: Instance, lockHandle: LockHandle) {
        instance.awaitIdle()
        pushInstance(instance.instanceId, instance, lockHandle)
        Persistence.store.unlock(instance.instanceId, lockHandle)
    }

    private suspend fun pushInstance(
        instanceId: String, instance: Instance, lockHandle: LockHandle
    ) {
        val record = InstanceRecord.fromInstance(instance)
        Persistence.store.set(instance.instanceId, record.toString(), lockHandle)
        println("[$instanceId] Persisted.")
    }
}

class ReadOnlyEncodingProxy(
    private val initialJsonString: String?,
    initialDecodedObject: Any?,
    initialSerializer: KSerializer<*>? // Can be null if not known at construction
) {
    private var _decodedValue: Any? = initialDecodedObject
    private var _cachedSerializer: KSerializer<*>? = initialSerializer // Store the serializer

    private val lock = Any() // For thread-safety

    companion object {
        val UNINITIALIZED_VALUE = {}

        /**
         * Creates a proxy initialized with a JSON string.
         * The actual object will be deserialized and its serializer determined on the first call to `getValue`.
         */
        fun fromJson(jsonString: String): ReadOnlyEncodingProxy {
            return ReadOnlyEncodingProxy(
                initialJsonString = jsonString,
                initialDecodedObject = UNINITIALIZED_VALUE,
                initialSerializer = null // Serializer will be determined by getValue
            )
        }

        /**
         * Creates a proxy initialized with an already decoded object and its explicit serializer.
         * This is the most robust way to use fromDecoded if getEncodedValue might be called
         * before getValue.
         *
         * @param decodedObject The pre-decoded object.
         * @param serializer The KSerializer for the type T.
         */
        fun <T> fromDecoded(
            decodedObject: T, property: KProperty<T>
        ): ReadOnlyEncodingProxy {
            if (decodedObject === UNINITIALIZED_VALUE) {
                throw IllegalArgumentException("Cannot initialize fromDecoded with UNINITIALIZED_VALUE.")
            }
            return ReadOnlyEncodingProxy(
                initialJsonString = null,
                initialDecodedObject = decodedObject,
                initialSerializer = getSerializer(property) // Store the provided serializer
            )
        }
    }

    @Suppress("UNCHECKED_CAST") operator fun <T> getValue(thisRef: Any?, property: KProperty<*>): T {
        val currentDecoded = _decodedValue

        if (currentDecoded !== UNINITIALIZED_VALUE) {
            // Value was pre-decoded (from fromDecoded) or already decoded by another thread.
            // Ensure serializer is cached if it wasn't provided or inferred during construction.
            if (_cachedSerializer == null) {
                synchronized(lock) { // Protect _cachedSerializer initialization
                    if (_cachedSerializer == null) { // Double-check idiom
                        val kType = property.returnType
                        _cachedSerializer = Json.serializersModule.serializer(kType)
                        println("Delegated property '${getFullPropertyName(property)}': Lazily initialized serializer via KProperty for pre-decoded object on first getValue().")
                    }
                }
            }
            return currentDecoded as T
        }

        // If we reach here, _decodedValue was UNINITIALIZED_VALUE (needs decoding from JSON)
        return synchronized(lock) {
            // Double-check idiom for thread-safe lazy initialization
            if (_decodedValue === UNINITIALIZED_VALUE) {
                val jsonToDecode = initialJsonString
                requireNotNull(jsonToDecode) {
                    "Internal inconsistency: Value is UNINITIALIZED but no initialJsonString was provided."
                }

                println("Delegated property '${getFullPropertyName(property)}': Decoding from JSON: \"$jsonToDecode\"")

                val kType: KType = property.returnType
                // This is the definitive serializer based on the property's type
                val resolvedSerializer = Json.serializersModule.serializer(kType)
                _cachedSerializer = resolvedSerializer // Cache it

                val decoded = Json.decodeFromString(resolvedSerializer, jsonToDecode)
                _decodedValue = decoded
                decoded as T
            } else {
                // Another thread initialized it while this one was waiting for the lock.
                // The serializer should have been set by the thread that performed the decoding.
                // For robustness, ensure _cachedSerializer is set if property type is available.
                // This path implies _decodedValue is set, so a previous getValue must have occurred or fromDecoded was used.
                if (_cachedSerializer == null) {
                    // This state (decoded but no serializer) is less likely if fromDecoded always tries or
                    // the decoding thread sets it. But as a safeguard:
                    val kType = property.returnType
                    _cachedSerializer = Json.serializersModule.serializer(kType)
                    println("Delegated property '${getFullPropertyName(property)}': Serializer was missing but re-resolved in getValue() (concurrent init path).")
                }
                _decodedValue as T
            }
        }
    }

    /**
     * Returns the proxied value as a JSON string.
     *
     * If the proxy was initialized with a JSON string and `getValue` has not yet been called,
     * the original JSON string is returned.
     * Otherwise, the currently held (decoded) value is serialized back to a JSON string using
     * the cached KSerializer.
     *
     * @throws IllegalStateException if the value is decoded but no KSerializer could be determined
     * (e.g., initialized with fromDecoded<Any>(...) and getValue was never called).
     * @return The JSON string representation of the value.
     */
    fun getEncodedValue(): String {
        synchronized(lock) {
            // Case 1: Initialized with JSON, and getValue() has NOT been called yet.
            if (_decodedValue === UNINITIALIZED_VALUE) {
                return initialJsonString ?: throw IllegalStateException(
                    "Internal inconsistency: Value is UNINITIALIZED, but no initialJsonString was stored."
                )
            }

            // Case 2: Value has been decoded or was provided pre-decoded.
            val valueToEncode = _decodedValue
            val serializerToUse = _cachedSerializer

            if (serializerToUse == null) {
                // This means:
                // 1. fromJson() was used, then getValue() was called, but _cachedSerializer somehow wasn't set (internal error).
                // OR
                // 2. fromDecoded<T>() was used, T was too generic for serializer inference (e.g. Any),
                //    _cachedSerializer remained null, AND getValue() has not been called yet to resolve it via KProperty.
                throw IllegalStateException(
                    "Cannot encode value: Serializer is not available. " + "If initialized with fromJson(), this is an internal error. " + "If initialized with fromDecoded() and serializer inference failed (e.g. for type 'Any'), " + "ensure getValue() is called at least once before getEncodedValue(), " + "or use the fromDecoded() overload that accepts an explicit KSerializer."
                )
            }

            println("Encoding current value using cached serializer. Value: $valueToEncode")

            // Helper function for type-safe encoding with the KSerializer<*>
            @Suppress("UNCHECKED_CAST") fun <ActualT> internalEncode(
                serializer: KSerializer<ActualT>,
                value: Any?
            ): String {
                return Json.encodeToString(serializer, value as ActualT)
            }

            return internalEncode(serializerToUse, valueToEncode)
        }
    }
}

fun <T> getSerializer(property: KProperty<T>): KSerializer<T> {
    val kType: KType = property.returnType
    // Get the serializer for the specific type T of the property
    return Json.serializersModule.serializer(kType) as KSerializer<T>
}

@Serializable
class InteractionRecord(
    val interactionId: String, val dependencies: Set<String>, val targets: Set<String>
)

@Serializable
class InstanceRecord(
    val instanceState: Map<String, String>,
    val initialRunPerformed: Boolean,
    val interactionRecords: List<InteractionRecord>
) {
    companion object {
        fun fromInstance(instance: Instance): InstanceRecord {
            return InstanceRecord(
                instanceState = instance.instanceState.mapValues { it.value.getEncodedValue() },
                initialRunPerformed = instance.initialRunPerformed,
                interactionRecords = instance.interactions.map { interaction ->
                    InteractionRecord(
                        interaction.name,
                        interaction.dependencies,
                        interaction.targets,
                    )
                })
        }

        fun fromString(jsonString: String): InstanceRecord {
            return Json.decodeFromString<InstanceRecord>(jsonString)
        }
    }

    override fun toString(): String {
        return Json.encodeToString(this)
    }
}

class InstanceHandle(
    private val instanceId: String
) {
    /**
     * Provides a scope function `instance { ... }` syntax.
     * Ensures the code block executes within the context of this specific Instance.
     */
    suspend operator fun invoke(block: suspend Instance.() -> Unit): Result<Unit> {
        try {
            withInstance(instanceId) { instanceReference ->
                instanceReference.use { ref ->
                    ref.instance {
                        block()
                    }
                }
            }
            return Result.success(Unit)
        } catch (t: Throwable) {
            return Result.failure(t)
        }
    }
}

suspend fun withInstance(instanceId: String, block: suspend (instanceReference: InstanceReference) -> Unit) {
    val instanceReferenceResult = Instance.instanceCache.lookup(instanceId)
    val instanceReference = instanceReferenceResult.getOrThrow()
    return block(instanceReference)
}