package dsl.components

import kotlinx.coroutines.ThreadContextElement
import kotlinx.coroutines.asContextElement
import kotlinx.coroutines.withContext
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import kotlin.reflect.KProperty

/**
 * Registry for managing and looking up registered reactions by name.
 */
object ReactionRegistry {
    private val registeredReactions: ConcurrentHashMap<String, Reaction> = ConcurrentHashMap()

    internal fun register(reaction: Reaction) {
        val existing = registeredReactions.putIfAbsent(reaction.name, reaction)
        if (existing != null) {
            throw IllegalArgumentException("Reaction with name '${reaction.name}' is already registered.")
        }
        println("Registered reaction: ${reaction.name}")
    }

    fun getByName(name: String): Reaction? {
        return registeredReactions[name]
    }
}

/**
 * Defines a reaction.
 * @param name An optional descriptive name for the reaction.
 * @param implementation The suspendable lambda function containing the reaction's logic.
 *                     Property reads/writes inside this lambda are tracked.
 */
fun reaction(
    name: String = UUID.randomUUID().toString(), implementation: suspend ReactionScope.() -> Unit
): Reaction {
    val newReaction = Reaction(name, implementation)
    // Register the reaction immediately upon definition
    ReactionRegistry.register(newReaction)
    return newReaction
}

class Reaction(
    val name: String, private val implementation: suspend ReactionScope.() -> Unit
) {
    var dependencies: MutableSet<String> = ConcurrentHashMap.newKeySet()

    var targets: MutableSet<String> = ConcurrentHashMap.newKeySet()

    suspend fun invoke() {
        // This should always be non-null when invoke is called correctly
        val currentInstance = InstanceContext.getCurrentInstance()
            ?: throw IllegalStateException("Reaction.invoke called outside of a valid Instance context.")

        // Create the scope object containing the ID
        val reactionScope = ReactionScope(currentInstance.instanceId)

        // Establish the reaction context for dependency tracking
        val contextElement = ReactionContext.coroutineContextElement(this)

        // Run the implementation ensuring the ReactionContext is present.
        withContext(contextElement) {
            reactionScope.implementation()
        }
    }

    // Internal: called by PropertyAccess.getValue
    fun addDependency(property: KProperty<*>) {
        dependencies.add(getFullPropertyName(property))
    }

    // Internal: called by PropertyAccess.setValue
    fun addTarget(property: KProperty<*>) {
        targets.add(getFullPropertyName(property))
    }
}

// Context manager to track the currently executing Reaction.
object ReactionContext {
    private val currentReaction = ThreadLocal<Reaction?>()

    // Returns the Reaction associated with the current coroutine, or null if none.
    fun getCurrentReaction(): Reaction? {
        return currentReaction.get()
    }

    // Helper to create a coroutine context element that preserves the current Reaction.
    fun coroutineContextElement(reaction: Reaction): ThreadContextElement<Reaction?> {
        return currentReaction.asContextElement(reaction)
    }
}

// Provides context to the reaction lambda.
class ReactionScope(
    // The unique identifier of the Instance currently executing this reaction.
    val instanceId: String
)