package dsl

import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.reflect.KProperty
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

// State Stores

// --- Configuration Data Classes ---
sealed class PersistenceConfig {
    /** Uses a simple ConcurrentHashMap, state lost on application exit. */
    data object InMemory : PersistenceConfig()

    /** Connects to an external Redis server. */
    data class Redis(
        val host: String = "localhost",
        val port: Int = 6379,
        val database: Int = 0,
        val password: String? = null,
        // Could add options for SSL, timeouts, pool size etc.
        // val connectionString: String? = null // Alternative for complex URIs
    ) : PersistenceConfig()
}

// --- Central Persistence Manager ---

object Persistence {
    // Default to in-memory
    @Volatile // Ensure visibility across threads
    private var _store: LockableKeyValueStore = InMemoryLockableKeyValueStore() // Start with default

    // Public accessor
    // TODO: should make note of when first accessed. So that we can throw warning when reconfigured after accessed.
    val store: LockableKeyValueStore get() = _store // Return the currently configured store

    /**
     * Configures the persistence layer for the Recipe DSL.
     * Must be called *once* before any recipes are baked or accessed,
     * typically at application startup.
     *
     * @param config The desired persistence configuration.
     * @throws IllegalStateException if persistence has already been configured.
     */
    fun configure(config: PersistenceConfig) {
        // TODO
    }
}

// --- Persistence Interface ---

// --- Lock-related Components ---

/**
 * Represents an acquired lock. This handle must be used for subsequent
 * operations on the locked key and for unlocking it.
 */
data class LockHandle(internal val key: String)

// --- Lockable KeyValueStore Interface ---
interface LockableKeyValueStore {

    suspend fun lock(key: String, timeout: Duration = 5.seconds): Result<LockHandle>

    suspend fun unlock(key: String, lockHandle: LockHandle): Result<Unit>

    suspend fun exists(key: String): Result<Boolean>

    suspend fun get(key: String, lockHandle: LockHandle): Result<Instance>

    suspend fun set(key: String, value: Instance, lockHandle: LockHandle): Result<Unit>

    suspend fun delete(key: String, lockHandle: LockHandle): Result<Unit>
}

// --- In-Memory Implementation of LockableKeyValueStore ---
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

// Context manager to track current recipe instance
object RecipeContext {
    private val currentInstance = ThreadLocal<Instance>()

    // Returns the RecipeInstance associated with the current coroutine, or null if none.
    fun getCurrentInstance(): Instance? {
        return currentInstance.get()
    }

    // Helper method to create a coroutine context element for propagating the RecipeInstance.
    fun coroutineContextElement(instance: Instance): ThreadContextElement<Instance?> {
        @Suppress("UNCHECKED_CAST") // Safe cast due to how asContextElement works
        return currentInstance.asContextElement(instance) as ThreadContextElement<Instance?>
    }
}

// Provides context to the interaction lambda.
class InteractionScope(
    // The unique identifier of the RecipeInstance currently executing this interaction.
    val recipeInstanceId: String
)

// Context manager to track the currently executing Interaction.
object InteractionContext {
    private val currentInteraction = ThreadLocal<Interaction?>()

    // Returns the Interaction associated with the current coroutine, or null if none.
    fun getCurrentInteraction(): Interaction? {
        return currentInteraction.get()
    }

    // Helper to create a coroutine context element that preserves the current Interaction.
    fun coroutineContextElement(interaction: Interaction): ThreadContextElement<Interaction?> {
        return currentInteraction.asContextElement(interaction)
    }
}

/**
 * Represents a single reactive unit of work within a recipe.
 * It automatically tracks property reads (dependencies) and writes (targets)
 * performed within its `implementation` lambda.
 * Uses ConcurrentHashMap's keySet for thread-safe dependency/target sets.
 */
class Interaction(
    val name: String, private val implementation: suspend InteractionScope.() -> Unit
) {
    // Properties read within `implementation`. Automatically populated.
    val dependencies: MutableSet<KProperty<*>> = ConcurrentHashMap.newKeySet()

    // Properties written within `implementation`. Automatically populated.
    val targets: MutableSet<KProperty<*>> = ConcurrentHashMap.newKeySet()

    /**
     * Executes the interaction's logic within the correct InteractionContext,
     * ensuring dependency/target tracking works correctly even across suspensions.
     */
    suspend fun invoke() {
        // This should always be non-null when invoke is called correctly by RecipeInstance
        val currentInstance = RecipeContext.getCurrentInstance()
            ?: throw IllegalStateException("Interaction.invoke called outside of a valid RecipeInstance context.")

        // Create the scope object containing the ID
        val interactionScope = InteractionScope(currentInstance.instanceId)

        // Establish the Interaction context for dependency tracking
        val contextElement = InteractionContext.coroutineContextElement(this)

        // Run the implementation ensuring the InteractionContext is present.
        withContext(contextElement) {
            interactionScope.implementation()
        }
    }

    // Internal: called by PropertyAccess.getValue
    fun addDependency(property: KProperty<*>) {
        dependencies.add(property)
    }

    // Internal: called by PropertyAccess.setValue
    fun addTarget(property: KProperty<*>) {
        targets.add(property)
    }
}

/**
 * Defines an interaction.
 * @param name An optional descriptive name for the interaction.
 * @param implementation The suspendable lambda function containing the interaction's logic.
 *                     Property reads/writes inside this lambda are tracked.
 */
fun interaction(
    name: String = UUID.randomUUID().toString(), implementation: suspend InteractionScope.() -> Unit
): Interaction {
    return Interaction(name, implementation)
}

/**
 * Utility to get a clean string representation of a property (e.g., "MyStore.a").
 */
fun getFullPropertyName(property: KProperty<*>): String {
    // Extracts the part like "val MyStore.a: kotlin.Int" -> "MyStore.a"
    return property.toString().split(" ")[1].substringBefore(':')
}

/**
 * A running instance of a recipe definition. Manages its own state,
 * work queue, and execution lifecycle independently of other instances.
 */
class Instance(
    val instanceId: String, private val interactions: Set<Interaction>
) {
    // --- Companion Object for Registry ---
    companion object {
        // Create a dedicated CoroutineScope for the InstanceCache.
        // This scope should live as long as the InstanceCache is needed, typically application lifetime.
        // SupervisorJob ensures that failure of one child coroutine (e.g., a finalize operation)
        // doesn't cancel the entire scope, allowing other cache operations to continue.
        private val instanceCacheSupervisorJob = SupervisorJob()
        private val instanceCacheCoroutineScope = CoroutineScope(
            Dispatchers.Default + // Suitable for general purpose async tasks.
                    // Suspend functions within Persistence.store should handle their own I/O dispatching if needed.
                    instanceCacheSupervisorJob +
                    CoroutineName("InstanceCacheGlobalScope") // Descriptive name for debugging
        )

        internal val instanceCache = InstanceCache(
            coroutineScope = instanceCacheCoroutineScope
        )
    }

    /**
     * Stores the current value of all 'ingredient' properties for this specific recipe instance.
     * Key: The KProperty object representing the ingredient (e.g., `MyStore::a`).
     * Value: The actual value of the ingredient (e.g., 10, "hello"), stored as Any?
     *
     * Uses ConcurrentHashMap for thread-safety during concurrent interaction execution.
     */
    internal val instanceState = ConcurrentHashMap<KProperty<*>, Any?>()

    // Thread-safe queue for properties that have been updated and need processing.
    private val workList = ConcurrentLinkedQueue<KProperty<*>>()

    // Mutex to synchronize access to the worker Job and idle state management.
    private val workerMutex = Mutex()
    private var worker: Job? = null

    // Deferred completed when the worker becomes idle (no work pending).
    @Volatile private var idleDeferred = CompletableDeferred<Unit>().apply { complete(Unit) } // Start idle

    // Ensures the initial run (processing all interactions) happens only once.
    private var initialRunPerformed = false

    // Dedicated scope for this instance's coroutines, automatically propagating its RecipeContext.
    private val scope = CoroutineScope(
        Dispatchers.Default + RecipeContext.coroutineContextElement(this) + CoroutineName("RecipeInstance-$instanceId")
    )

    /**
     * Ensures the background worker coroutine is running if it's not already
     * active and there's pending work or the initial run hasn't occurred.
     */
    private fun ensureWorkerIsRunning() {
        // Launch non-blockingly within the instance's scope.
        scope.launch {
            workerMutex.withLock {
                // Start a new worker only if none is active.
                if (worker?.isActive != true) {
                    // If we are starting a new worker, it means we are transitioning from potentially
                    // idle to active. If the current idleDeferred is completed (we were idle),
                    // we need a new deferred for this new period of activity.
                    if (idleDeferred.isCompleted) {
                        println("[$instanceId] Worker starting, was idle. Resetting idleDeferred.")
                        idleDeferred = CompletableDeferred()
                    }

                    val workerNeedsInitialRun = !initialRunPerformed
                    if (workerNeedsInitialRun) initialRunPerformed = true // Mark before worker starts

                    println("[$instanceId] Starting worker. Initial run needed: $workerNeedsInitialRun")
                    worker = scope.launch(CoroutineName("Worker-$instanceId")) {
                        // The worker coroutine processes updates and manages idle state.
                        processWorkList(workerNeedsInitialRun)
                    }
                }
            }
        }
    }

    /**
     * The main loop of the background worker coroutine. Processes property updates
     * from the workList and executes dependent interactions. Manages the idle state.
     */
    private suspend fun processWorkList(performInitialRun: Boolean) {
        try {
            // Ensure RecipeContext is available throughout the worker's execution.
            withContext(RecipeContext.coroutineContextElement(this@Instance)) {
                if (performInitialRun) {
                    println("[$instanceId] Performing initial run (all interactions).")
                    runIteration(null) // Null property triggers all interactions.
                }

                // Process work items until the coroutine is cancelled.
                while (isActive) {
                    val propertyToProcess = workList.poll() // Non-blocking, thread-safe poll.

                    if (propertyToProcess != null) {
                        println("[$instanceId] Processing update for: ${getFullPropertyName(propertyToProcess)}")
                        runIteration(propertyToProcess)
                    } else {
                        // Work list is empty, attempt to transition to idle.
                        // Lock is crucial to ensure atomicity of checking emptiness and setting worker to null.
                        val shouldContinue = workerMutex.withLock {
                            // Double-check queue is *still* empty after acquiring lock.
                            if (workList.isEmpty()) {
                                println("[$instanceId] Work queue empty, worker going idle.")
                                worker = null // Mark worker as inactive *before* completing deferred.
                                idleDeferred.complete(Unit)
                                false // Signal loop to stop.
                            } else {
                                true // Item arrived between poll() and lock acquisition, continue processing.
                            }
                        }

                        if (!shouldContinue) {
                            println("[$instanceId] Worker loop exiting (idle).")
                            break // Exit the while loop.
                        } else {
                            // Queue wasn't empty after check, yield to avoid potential busy-waiting.
                            yield()
                        }
                    }
                } // end while(isActive)
            } // end withContext(RecipeContext)
        } catch (e: CancellationException) {
            println("[$instanceId] Worker cancelled.")
            workerMutex.withLock { // Safely update state during cancellation.
                if (worker == currentCoroutineContext().job) { // Check if this is still the current worker.
                    worker = null
                    idleDeferred.completeExceptionally(e) // Complete current deferred exceptionally.
                }
            }
            throw e // Re-throw cancellation.
        } catch (e: Throwable) {
            println("[$instanceId] Worker failed: ${e.message}")
            e.printStackTrace()
            workerMutex.withLock { // Safely update state on failure.
                if (worker == currentCoroutineContext().job) {
                    worker = null
                    idleDeferred.completeExceptionally(e) // Complete current deferred exceptionally.
                }
            }
        }
    }

    /**
     * Internal method called by [PropertyAccess.setValue] when an ingredient changes.
     * Adds the property to the work queue and ensures the worker is running.
     */
    fun notifyUpdate(property: KProperty<*>) {
        println("[$instanceId] Notified update for: ${getFullPropertyName(property)}")
        workList.offer(property) // Thread-safe add to queue.
        ensureWorkerIsRunning() // Wake up worker if idle.
    }

    /**
     * Runs all interactions that depend on the given property.
     * If the property is null, runs all interactions (initial run).
     */
    private suspend fun runIteration(property: KProperty<*>? = null) {
        // Find interactions where the updated property is a dependency, or all if property is null.
        val dependentInteractions = interactions.filter { interaction ->
            property == null || property in interaction.dependencies
        }

        if (dependentInteractions.isNotEmpty()) {
            val type = if (property == null) "initial" else "update on ${getFullPropertyName(property)}"
            println("[$instanceId] Running ${dependentInteractions.size} interactions for $type")

            // Run interactions concurrently within a child scope that inherits the RecipeInstance context.
            coroutineScope {
                dependentInteractions.forEach { interaction ->
                    launch(CoroutineName("Interaction-${interaction.name}")) {
                        try {
                            interaction.invoke() // Executes the interaction's logic + context setup.
                        } catch (e: Exception) {
                            // Log errors within interactions. Consider more robust error handling/reporting.
                            println("[$instanceId] Error in interaction '${interaction.name}': ${e.message}")
                            e.printStackTrace() // TODO: Replace with proper logging/error handling
                        }
                    }
                }
            }
            println("[$instanceId] Finished running interactions for $type")
        } else {
            val type = if (property == null) "initial" else "update on ${getFullPropertyName(property)}"
            println("[$instanceId] No interactions to run for $type")
        }
    }


    /**
     * Suspends until the worker coroutine has processed all currently queued work
     * and becomes idle. If new work arrives while waiting, it will be processed
     * before this function resumes.
     */
    suspend fun awaitIdle() {
        val deferredToAwait: CompletableDeferred<Unit> = workerMutex.withLock {
            if (idleDeferred.isCompleted && (worker?.isActive == true || !workList.isEmpty())) {
                idleDeferred = CompletableDeferred()
            }
            idleDeferred
        }
        println("[$instanceId] Awaiting idle state...")
        deferredToAwait.await() // Suspend until the worker completes the deferred.
        println("[$instanceId] Resumed from idle.")
    }


    /**
     * Provides a scope function `recipeInstance { ... }` syntax.
     * Ensures the code block executes within the context of this specific RecipeInstance.
     */
    suspend operator fun invoke(block: suspend Instance.() -> Unit) {
        // Set the RecipeInstance context for the duration of the block.
        withContext(RecipeContext.coroutineContextElement(this)) {
            this@Instance.block()
        }
    }

    /**
     * Prints a simple textual visualization of the recipe's structure (interactions and data flow).
     * Note: This shows the static structure, not the current runtime state.
     */
    fun visualize() {
        println("\n=== Recipe Graph for $instanceId ===")
        val interactions = interactions
        val allIngredients = mutableSetOf<KProperty<*>>()
        interactions.forEach {
            allIngredients.addAll(it.dependencies)
            allIngredients.addAll(it.targets)
        }

        println("\nIngredients (Properties):")
        allIngredients.forEach { println("  * ${getFullPropertyName(it)}") }

        println("\nInteractions:")
        interactions.forEach { interaction ->
            val name = interaction.name
            val inputs = interaction.dependencies.joinToString(", ") { getFullPropertyName(it) }
            val outputs = interaction.targets.joinToString(", ") { getFullPropertyName(it) }
            println("  * $name")
            println("    - Reads: ${if (inputs.isEmpty()) "none" else inputs}")
            println("    - Writes: ${if (outputs.isEmpty()) "none" else outputs}")
        }

        println("\nData Flow:")
        interactions.forEach { interaction ->
            val deps = interaction.dependencies
            val depStr = if (deps.isEmpty()) "(no inputs)" else deps.joinToString(", ") { getFullPropertyName(it) }
            val targets = interaction.targets
            val targetStr =
                if (targets.isEmpty()) "(no outputs)" else targets.joinToString(", ") { getFullPropertyName(it) }
            println("  $depStr --> [${interaction.name}] --> $targetStr")
        }
        println("\n=== End of Recipe Graph ===")
    }
}

/**
 * Property delegate for ingredients. Manages instance-specific values and triggers
 * recipe instance updates upon modification. Automatically tracks dependencies/targets
 * when accessed within an Interaction context.
 */
class PropertyAccess<T>(private val defaultValue: T) {
    /**
     * Gets the value for the current RecipeInstance.
     * If accessed within an Interaction, registers the property as a dependency.
     * Throws IllegalStateException if accessed outside a RecipeInstance context.
     */
    operator fun getValue(thisRef: Any?, property: KProperty<*>): T {
        val currentInteraction = InteractionContext.getCurrentInteraction()
        val currentInstance = RecipeContext.getCurrentInstance() ?: throw IllegalStateException(
            "Cannot get ingredient value outside of a RecipeInstance context for property ${
                getFullPropertyName(
                    property
                )
            }"
        )

        // Track dependency if we are inside an interaction's execution context.
        currentInteraction?.addDependency(property)

        // --- Access the central state store in RecipeInstance ---
        // Use computeIfAbsent for atomicity and default value handling.
        // The key is the KProperty itself.
        val value = currentInstance.instanceState.computeIfAbsent(property) {
            println("[${currentInstance.instanceId}] Using default value for ${getFullPropertyName(property)}")
            defaultValue // Use the property-specific default
        }

        // --- Crucial Type Cast ---
        // We expect the value stored in instanceState for this KProperty to be of type T.
        // This relies on setValue always storing the correct type.
        // Serialization/deserialization must ensure type correctness when loading state.
        try {
            @Suppress("UNCHECKED_CAST") // Justification: Internal DSL consistency ensures correct type stored by setValue
            return value as T
        } catch (e: ClassCastException) {
            // This indicates a potential bug or corrupted state (e.g., bad deserialization)
            throw IllegalStateException("Type mismatch for property ${getFullPropertyName(property)} in instance ${currentInstance.instanceId}. Expected ${defaultValue!!::class.simpleName} but found ${value?.let { it::class.simpleName } ?: "null"}.",
                e)
        }
    }

    /**
     * Sets the value for the current RecipeInstance.
     * If set within an Interaction, registers the property as a target.
     * Notifies the current RecipeInstance of the update, triggering re-evaluation.
     * Throws IllegalStateException if accessed outside a RecipeInstance context.
     */
    operator fun setValue(thisRef: Any?, property: KProperty<*>, newValue: T) {
        val currentInteraction = InteractionContext.getCurrentInteraction()
        val currentInstance = RecipeContext.getCurrentInstance() ?: throw IllegalStateException(
            "Cannot set ingredient value outside of a RecipeInstance context for property ${
                getFullPropertyName(
                    property
                )
            }"
        )

        // Track target if we are inside an interaction's execution context.
        currentInteraction?.addTarget(property)

        // --- Update the central state store in RecipeInstance ---
        // The KProperty is the key, newValue (of type T, stored as Any?) is the value.
        // ConcurrentHashMap put is thread-safe.
        currentInstance.instanceState.put(property, newValue)

        // Notify the instance that this property has changed.
        currentInstance.notifyUpdate(property)
    }
}

/**
 * Base class for defining data stores containing ingredients.
 * Provides the `ingredient` delegate function.
 */
open class Store {
    /**
     * Delegate function to create an ingredient property.
     * @param initialValue The default value for the ingredient in new RecipeInstances.
     */
    protected fun <T> ingredient(initialValue: T) = PropertyAccess(initialValue)
}

// LOCAL INSTANCE CACHE

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
            println("Increased refCount for ${instance.instanceId} to $refCount")
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
            println("Decreased refCount for ${instance.instanceId} to $refCount")

            if (refCount == 0) {
                // Attempt to remove THIS entry from the cache.
                if (ownerCache.atomicallyRemoveEntryFromCache(instance.instanceId, this)) {
                    shouldFinalizeRemoval = true
                    instanceToFinalize = this.instance
                    println("Entry for ${instance.instanceId} marked for DB finalization.")
                } else {
                    // Entry was not removed by us. Could be:
                    // 1. Already removed by another thread/process.
                    // 2. Replaced in the cache by a different entry for the same ID (less likely with current design but possible if cache had explicit overwrite).
                    // In this case, this specific entry instance should not trigger DB persistence.
                    println("Entry for ${instance.instanceId} was not in cache or was a different entry object when trying to remove for finalization.")
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
     * Provides a scope function `recipeInstance { ... }` syntax.
     * Ensures the code block executes within the context of this specific RecipeInstance.
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

suspend fun createInstance(
    instanceId: String, interactions: Set<Interaction> = emptySet(), expiresAfter: Duration = Duration.INFINITE
): InstanceHandle {
    val instance = Instance(instanceId, interactions)
    Instance.instanceCache.register(instance)
    return InstanceHandle(instanceId)
}

suspend fun String.findInstance(): Result<InstanceHandle> {
    if (Persistence.store.exists(this).getOrThrow()) {
        return Result.success(InstanceHandle(this))
    }
    return Result.failure(IllegalStateException("No instance with identifier ($this) exists."))
}

