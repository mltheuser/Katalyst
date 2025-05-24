package dsl.components

import dsl.persistance.InstanceCache
import dsl.persistance.InstanceHandle
import dsl.persistance.Persistence
import dsl.persistance.ReadOnlyEncodingProxy
import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import kotlin.reflect.KProperty
import kotlin.time.Duration

suspend fun createInstance(
    instanceId: String, interactions: Set<Interaction> = emptySet(), expiresAfter: Duration = Duration.INFINITE
): Result<InstanceHandle> {
    val instance = Instance(instanceId, interactions, expiresAfter)
    return Instance.instanceCache.register(instance).map {
        InstanceHandle(instanceId)
    }
}

suspend fun String.lookupInstance(): Result<InstanceHandle> {
    if (Persistence.store.exists(this).getOrThrow()) {
        return Result.success(InstanceHandle(this))
    }
    return Result.failure(IllegalStateException("No instance with identifier ($this) exists."))
}

suspend fun findInstances(pattern: String): Result<Iterator<InstanceHandle>> {
    val matchingKeysResult: Result<Iterator<String>> = Persistence.store.findKeysByPattern(pattern)

    return matchingKeysResult.map { stringIterator ->
        stringIterator.asSequence().map { instanceId -> InstanceHandle(instanceId) }.iterator()
    }
}

data class PropertyUpdate<T>(
    val property: KProperty<T>,
    val newValue: T,
)

/**
 * A running instance. Manages its own state,
 * work queue, and execution lifecycle independently of other instances.
 */
class Instance(
    val instanceId: String, internal val interactions: Set<Interaction>, val expiresAfter: Duration
) {

    init {
        // Validate expiration duration
        if (expiresAfter.isNegative()) {
            throw IllegalArgumentException("expiresAfter must be a positive duration.")
        }
    }

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
                    instanceCacheSupervisorJob + CoroutineName("InstanceCacheGlobalScope") // Descriptive name for debugging
        )

        internal val instanceCache = InstanceCache(
            coroutineScope = instanceCacheCoroutineScope
        )
    }

    /**
     * Stores the current value of all 'ingredient' properties for this specific instance.
     * Key: The KProperty object representing the ingredient (e.g., `MyStore::a`).
     * Value: The actual value of the ingredient (e.g., 10, "hello"), stored as Any?
     *
     * Uses ConcurrentHashMap for thread-safety during concurrent interaction execution.
     */
    internal val instanceState = ConcurrentHashMap<String, ReadOnlyEncodingProxy>()

    // Thread-safe queue for properties that have been updated and need processing.
    private val workList = ConcurrentLinkedQueue<PropertyUpdate<*>>()

    // Mutex to synchronize access to the worker Job and idle state management.
    private val workerMutex = Mutex()
    private var worker: Job? = null

    // Deferred completed when the worker becomes idle (no work pending).
    @Volatile private var idleDeferred = CompletableDeferred<Unit>().apply { complete(Unit) } // Start idle

    // Ensures the initial run (processing all interactions) happens only once.
    var initialRunPerformed = false

    // Dedicated scope for this instance's coroutines, automatically propagating its InstanceContext.
    private val scope = CoroutineScope(
        Dispatchers.Default + InstanceContext.coroutineContextElement(this) + CoroutineName("Instance-$instanceId")
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
            // Ensure InstanceContext is available throughout the worker's execution.
            withContext(InstanceContext.coroutineContextElement(this@Instance)) {
                if (performInitialRun) {
                    println("[$instanceId] Performing initial run (all interactions).")
                    runIteration(null) // Null property triggers all interactions.
                }

                // Process work items until the coroutine is cancelled.
                while (isActive) {
                    val updateToProcess = workList.poll() // Non-blocking, thread-safe poll.

                    if (updateToProcess != null) {
                        println("[$instanceId] Processing update for: ${getFullPropertyName(updateToProcess.property)}")
                        runIteration(updateToProcess)
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
                }
            }
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
     * Internal method called by [Ingredient.setValue] when an ingredient changes.
     * Adds the property to the work queue and ensures the worker is running.
     */
    fun notifyUpdate(update: PropertyUpdate<*>) {
        println("[$instanceId] Notified update for: ${getFullPropertyName(update.property)}")
        workList.offer(update) // Thread-safe add to queue.
        ensureWorkerIsRunning() // Wake up worker if idle.
    }

    /**
     * Runs all interactions that depend on the given property.
     * If the property is null, runs all interactions (initial run).
     */
    private suspend fun runIteration(update: PropertyUpdate<*>? = null) {
        // Apply update to instance state
        // --- Update the central state store in Instance ---
        if (update != null) {
            instanceState.put(
                getFullPropertyName(update.property),
                ReadOnlyEncodingProxy.fromDecoded(update.newValue, update.property)
            )
        }

        // Find interactions where the updated property is a dependency, or all if property is null.
        val dependentInteractions = interactions.filter { interaction ->
            update == null || getFullPropertyName(update.property) in interaction.dependencies
        }

        if (dependentInteractions.isNotEmpty()) {
            val type = if (update == null) "initial" else "update on ${getFullPropertyName(update.property)}"
            println("[$instanceId] Running ${dependentInteractions.size} interactions for $type")

            // Run interactions concurrently within a child scope that inherits the Instance context.
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
            val type = if (update == null) "initial" else "update on ${getFullPropertyName(update.property)}"
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
     * Provides a scope function `instance { ... }` syntax.
     * Ensures the code block executes within the context of this specific Instance.
     */
    suspend operator fun invoke(block: suspend Instance.() -> Unit) {
        // Set the Instance context for the duration of the block.
        withContext(InstanceContext.coroutineContextElement(this)) {
            this@Instance.block()
        }
    }

    /**
     * Prints a simple textual visualization of the instance's interaction structure (interactions and data flow).
     * Note: This shows the static structure, not the current runtime state.
     */
    fun visualize() {
        println("\n=== Interactions Graph for $instanceId ===")
        val interactions = interactions
        val allIngredients = mutableSetOf<String>()
        interactions.forEach {
            allIngredients.addAll(it.dependencies)
            allIngredients.addAll(it.targets)
        }

        println("\nIngredients (Properties):")
        allIngredients.forEach { println("  * ${it}") }

        println("\nInteractions:")
        interactions.forEach { interaction ->
            val name = interaction.name
            val inputs = interaction.dependencies.joinToString(", ") { it }
            val outputs = interaction.targets.joinToString(", ") { it }
            println("  * $name")
            println("    - Reads: ${if (inputs.isEmpty()) "none" else inputs}")
            println("    - Writes: ${if (outputs.isEmpty()) "none" else outputs}")
        }

        println("\nData Flow:")
        interactions.forEach { interaction ->
            val deps = interaction.dependencies
            val depStr = if (deps.isEmpty()) "(no inputs)" else deps.joinToString(", ") { it }
            val targets = interaction.targets
            val targetStr =
                if (targets.isEmpty()) "(no outputs)" else targets.joinToString(", ") { it }
            println("  $depStr --> [${interaction.name}] --> $targetStr")
        }
        println("\n=== End of Graph ===")
    }
}

/**
 * Utility to get a clean string representation of a property (e.g., "MyStore.a").
 */
fun getFullPropertyName(property: KProperty<*>): String {
    // Extracts the part like "val MyStore.a: kotlin.Int" -> "MyStore.a"
    return property.toString().split(" ")[1].substringBefore(':')
}
