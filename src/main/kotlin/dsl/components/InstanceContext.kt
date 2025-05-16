package dsl.components

import kotlinx.coroutines.ThreadContextElement
import kotlinx.coroutines.asContextElement

// Context manager to track current instance
object InstanceContext {
    private val currentInstance = ThreadLocal<Instance>()

    // Returns the Instance associated with the current coroutine, or null if none.
    fun getCurrentInstance(): Instance? {
        return currentInstance.get()
    }

    // Helper method to create a coroutine context element for propagating the Instance.
    fun coroutineContextElement(instance: Instance): ThreadContextElement<Instance?> {
        @Suppress("UNCHECKED_CAST") // Safe cast due to how asContextElement works
        return currentInstance.asContextElement(instance) as ThreadContextElement<Instance?>
    }
}