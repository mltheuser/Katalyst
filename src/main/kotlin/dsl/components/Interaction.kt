package dsl.components

import kotlinx.coroutines.ThreadContextElement
import kotlinx.coroutines.asContextElement
import kotlinx.coroutines.withContext
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import kotlin.reflect.KProperty

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

class Interaction(
    val name: String, private val implementation: suspend InteractionScope.() -> Unit
) {
    val dependencies: MutableSet<KProperty<*>> = ConcurrentHashMap.newKeySet()

    val targets: MutableSet<KProperty<*>> = ConcurrentHashMap.newKeySet()

    suspend fun invoke() {
        // This should always be non-null when invoke is called correctly
        val currentInstance = InstanceContext.getCurrentInstance()
            ?: throw IllegalStateException("Interaction.invoke called outside of a valid Instance context.")

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

// Provides context to the interaction lambda.
class InteractionScope(
    // The unique identifier of the Instance currently executing this interaction.
    val instanceId: String
)