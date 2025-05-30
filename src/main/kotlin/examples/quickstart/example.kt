package examples.quickstart

import dsl.components.createInstance
import dsl.components.ingredient
import dsl.components.reaction
import dsl.persistance.Persistence
import dsl.persistance.PersistenceConfig
import dsl.persistance.delete
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking

// 1. Define ingredients
var a by ingredient(4) // Initial value 4
var b by ingredient(0) // Initial value 0
var logs by ingredient<String?>(null) // Initial value null

fun main() {

    Persistence.configure(PersistenceConfig.InMemory)

    val reactions = setOf(
        // Reactions depends on 'a' and 'b'
        reaction("Divide Something") {
            // Reading 'a' and 'b' automatically registers them as dependencies for this reaction.
            val currentA = a
            val currentB = b

            if (currentB != 0) {
                println("[$instanceId] Calculating division: $currentA / $currentB")
                // Writing to 'logs' automatically registers it as a target.
                // It also triggers notifyUpdate for 'logs' in the current Instance.
                logs = "$currentA/$currentB=${currentA / currentB}"
            } else {
                println("[$instanceId] Division by zero skipped.")
                logs = "Division by zero attempt" // This write also registers 'logs' as target.
            }
        },
        // Reaction depends on 'a'
        reaction("Log A Changes") {
            val currentA = a // Dependency
            println("[$instanceId] Got some new a value: $currentA")
        },
        // Reaction depends on 'b'
        reaction("Log B Changes") {
            val currentB = b // Dependency
            println("[$instanceId] Got some new b value: $currentB")
        },
        // Reaction depends on 'logs'
        reaction("On logs changed") {
            val currentLogs = logs // Dependency
            println("[$instanceId] Got some new logs value: $currentLogs")
        })

    // Use a parallel dispatcher for potentially concurrent instance processing.
    runBlocking(Dispatchers.Default) {
        // 3. Create instances. Each gets its own state and lifecycle.
        // Creating an instance registers it id in the persistence store but does not yet execute any reactions.
        println("--- Creating Instance 1 ---")
        val instance1 = createInstance("instance1", reactions).getOrThrow()
        println("--- Creating Instance 2 ---")
        val instance2 = createInstance("instance2", reactions).getOrThrow()

        println("\n--- Modifying Instance 1 ---")
        // Use the instance invoke operator to scope operations to instance1.
        instance1 { // Sets InstanceContext for instance1 for the block.
            println("[$instanceId] Setting b = 2")
            // Setting 'b' triggers an initial run of all reactions,
            // only after that b=2 is applied and all reactions dependent on b are rerun.
            b = 2
            // Wait until the updates triggered by setting b=2 are fully processed.
            awaitIdle()
            println("[$instanceId] Reading logs after setting b=2")
            // Read the state *specific to instance1*.
            val logs1 = logs
            println("[$instanceId] logs: $logs1") // Expected: "4/2=2"
        }

        println("\n--- Modifying Instance 2 ---")
        instance2 { // Sets InstanceContext for instance2.
            println("[$instanceId] Setting a = 10, b = 5")
            a = 10 // Triggers update for 'a' -> runs "Divide Something", "Log A Changes"
            b = 5 // Triggers update for 'b' -> runs "Divide Something", "Log B Changes"
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
            println("[$instanceId] Final state: a=${a}, b=${b}, logs=${logs}") // a=4, b=2, logs="4/2=2"
        }
        instance2 {
            println("[$instanceId] Final state: a=${a}, b=${b}, logs=${logs}") // a=20, b=5, logs="20/5=4"
        }

        println("\n--- Visualizing Instance Structure ---")
        // Visualize the static structure.
        instance1 {
            visualize()
        }
        // instance2.visualize() // Would print the identical structure graph.

        println("\n--- Delete Instances ---")
        // delete instance 1
        instance1.delete().onSuccess {
            // Invocation should fail after successful deletion
            instance1 {}
        }
        // delete instance 2
        instance2.delete()

    } // End runBlocking

    println("\n--- Main Finished ---")
}