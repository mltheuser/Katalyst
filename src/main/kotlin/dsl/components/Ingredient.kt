package dsl.components

import dsl.persistance.ReadOnlyEncodingProxy
import kotlin.reflect.KProperty


/**
 * Property delegate for ingredients. Manages instance-specific values and triggers
 * instance updates upon modification. Automatically tracks dependencies/targets
 * when accessed within a Reaction context.
 */
class Ingredient<T>(private val defaultValue: T) {
    /**
     * Gets the value for the current Instance.
     * If accessed within a Reaction, registers the property as a dependency.
     * Throws IllegalStateException if accessed outside a Instance context.
     */
    operator fun getValue(thisRef: Any?, property: KProperty<*>): T {
        val currentReaction = ReactionContext.getCurrentReaction()
        val currentInstance = InstanceContext.getCurrentInstance() ?: throw IllegalStateException(
            "Cannot get ingredient value outside of a Instance context for property ${
                getFullPropertyName(
                    property
                )
            }"
        )

        // Track dependency if we are inside a Reaction's execution context.
        currentReaction?.addDependency(property)

        // --- Access the central state store in Instance ---
        // Use computeIfAbsent for atomicity and default value handling.
        // The key is the KProperty itself.
        val value = currentInstance.instanceState.computeIfAbsent(getFullPropertyName(property)) {
            println("[${currentInstance.instanceId}] Using default value for ${getFullPropertyName(property)}")
            ReadOnlyEncodingProxy.fromDecoded(defaultValue, property) // Use the property-specific default
        }

        // --- Crucial Type Cast ---
        // We expect the value stored in instanceState for this KProperty to be of type T.
        // This relies on setValue always storing the correct type.
        // Serialization/deserialization must ensure type correctness when loading state.
        try {
            @Suppress("UNCHECKED_CAST") // Justification: Internal DSL consistency ensures correct type stored by setValue
            return value.getValue(thisRef, property) as T
        } catch (e: ClassCastException) {
            // This indicates a potential bug or corrupted state (e.g., bad deserialization)
            throw IllegalStateException("Type mismatch for property ${getFullPropertyName(property)} in instance ${currentInstance.instanceId}. Expected ${defaultValue!!::class.simpleName} but found ${value.let { it::class.simpleName } ?: "null"}.",
                e)
        }
    }

    /**
     * Sets the value for the current Instance.
     * If set within an Reaction, registers the property as a target.
     * Notifies the current Instance of the update, triggering re-evaluation.
     * Throws IllegalStateException if accessed outside a Instance context.
     */
    operator fun setValue(thisRef: Any?, property: KProperty<*>, newValue: T) {
        val currentReaction = ReactionContext.getCurrentReaction()
        val currentInstance = InstanceContext.getCurrentInstance() ?: throw IllegalStateException(
            "Cannot set ingredient value outside of a Instance context for property ${
                getFullPropertyName(
                    property
                )
            }"
        )

        // Track target if we are inside a Reaction's execution context.
        currentReaction?.addTarget(property)

        // Notify the instance that this property needs to change.
        currentInstance.notifyUpdate(PropertyUpdate(property, newValue))
    }
}

/**
 * Delegate function to create an ingredient property.
 * @param initialValue The default value for the ingredient in new Instances.
 */
fun <T> ingredient(initialValue: T) = Ingredient(initialValue)