package examples

import dsl.components.createInstance
import dsl.components.ingredient
import dsl.components.interaction
import dsl.persistance.Persistence
import dsl.persistance.PersistenceConfig
import dsl.persistance.delete
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking

// ----- USAGE EXAMPLE -----

// 1. Define ingredients
var a by ingredient(4) // Initial value 4
var b by ingredient(0) // Initial value 0
var logs by ingredient<String?>(null) // Initial value null

fun main() {

    Persistence.configure(PersistenceConfig.InMemory)

    val interactions = setOf(
        interaction("Divide Something") {
            // Reading 'a' and 'b' automatically registers them as dependencies for this interaction.
            val currentA = a
            val currentB = b

            if (currentB != 0) {
                println("[$instanceId] Calculating division: $currentA / $currentB")
                // Writing to 'logs' automatically registers it as a target.
                // It also triggers notifyUpdate for 'logs' in the current RecipeInstance.
                logs = "$currentA/$currentB=${currentA / currentB}"
            } else {
                println("[$instanceId] Division by zero skipped.")
                logs = "Division by zero attempt" // This write also registers 'logs' as target.
            }
        },
        // Interaction depends on 'a'
        interaction("Log A Changes") {
            val currentA = a // Dependency
            println("[$instanceId] Got some new a value: $currentA")
        },
        // Interaction depends on 'b'
        interaction("Log B Changes") {
            val currentB = b // Dependency
            println("[$instanceId] Got some new b value: $currentB")
        },
        // Interaction depends on 'logs'
        interaction("On logs changed") {
            val currentLogs = logs // Dependency
            println("[$instanceId] Got some new logs value: $currentLogs")
        })

    // Use a parallel dispatcher for potentially concurrent instance processing.
    runBlocking(Dispatchers.Default) {
        // 3. Bake instances of the recipe. Each gets its own state and lifecycle.
        println("--- Starting Instance 1 ---")
        // Baking creates the instance and schedules its initial run (executing all interactions once).
        val instance1 = createInstance("recipe1", interactions).getOrThrow()
        println("--- Starting Instance 2 ---")
        val instance2 = createInstance("recipe2", interactions).getOrThrow()

        println("\n--- Modifying Instance 1 ---")
        // Use the instance invoke operator to scope operations to instance1.
        instance1 { // Sets RecipeContext for instance1 for the block.
            println("[$instanceId] Setting b = 2")
            // Setting 'b' triggers notifyUpdate('b') -> worker processes 'b'.
            // The worker runs interactions dependent on 'b' ("Divide Something", "Log B Changes").
            b = 2
            // Wait until the updates triggered by setting b=2 are fully processed.
            // awaitIdle()
            println("[$instanceId] Reading logs after setting b=2")
            // Read the state *specific to instance1*.
            val logs1 = logs
            println("[$instanceId] logs: $logs1") // Expected: "4/2=2"
        }

        println("\n--- Modifying Instance 2 ---")
        instance2 { // Sets RecipeContext for instance2.
            println("[$instanceId] Setting a = 10, b = 5")
            a = 10 // Triggers update for 'a' -> runs "Divide Something", "Log A Changes"
            b = 5 // Triggers update for 'b' -> runs "Divide Something", "Log B Changes"
            // Note: "Divide Something" might run twice if processing isn't batched (current impl).
            awaitIdle() // Wait for processing of a=10, b=5.
            println("[$instanceId] Reading logs after setting a=10, b=5")
            println("[$instanceId] logs: ${logs}") // Expected: "10/5=2"

            println("[$instanceId] Setting a = 20")
            a = 20 // Triggers update for 'a' -> runs "Divide Something", "Log A Changes"
            awaitIdle() // Wait for processing of a=20.
            println("[$instanceId] Reading logs after setting a=20")
            val logs2 = logs
            println("[$instanceId] logs: $logs2") // Expected: "20/5=4"
        }

        println("\n--- Final State Check (Instance-Specific) ---")
        // Demonstrate that each instance maintains its separate state.
        instance1 {
            awaitIdle()
            println("[$instanceId] Final state: a=${a}, b=${b}, logs=${logs}") // a=4, b=2, logs="4/2=2"
        }
        instance2 {
            awaitIdle()
            println("[$instanceId] Final state: a=${a}, b=${b}, logs=${logs}") // a=20, b=5, logs="20/5=4"
        }

        println("\n--- Visualizing Recipe Structure ---")
        // Visualize the static structure (same for all instances of this recipe).
        instance1 {
            visualize()
        }
        // instance2.visualize() // Would print the identical structure graph.

        // delete instance
        instance1.delete().onSuccess {
            // Invocation should fail after successful deletion
            instance1 {}
        }

    } // End runBlocking

    println("\n--- Main Finished ---")
}