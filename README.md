# Katalyst <img src="https://github.com/user-attachments/assets/461a6e4b-aeaf-423c-811b-6c932e5d7abe" height="30px"/>

```
A library for building stateful and/or reactive services in Kotlin.
```

## Quickstart

First, to get a feel for the library, here is all the important syntax at one glance:

```Kotlin

// Define State
var a by ingredient(initialValue=4)
var b by ingredient(initialValue=0)
var c by ingredient<Int?>()

// Define Reactions
val reactions = setOf(
    reaction("divide a and b") {
        if (b != 0) {
            c = a / b
        } else {
            println("[$instanceId] Division by zero skipped.")
        }
    }
)

// Create Instances
val instance1 = createInstance("instance1", reactions).getOrThrow()

val instance2 = createInstance("instance2", reactions, expiresAfter = 10.minutes).getOrThrow()

// Update Instances
instance1 {
    b = 2
}
instance2 {
    a = 10
    b = 3
}

// Check Individual State
instance1 {
    awaitIdle()
    println("[$instanceId] state: a=${a}, b=${b}, c=${c}") // expect c=2
}
instance2 {
    awaitIdle()
    println("[$instanceId] state: a=${a}, b=${b}, c=${c}") // expect c=3
}

// Iterate over instances
findInstances("instance*").onSuccess { instances ->
    instances.forEach { instance ->
        println($instanceId) // <- prints "instance1" and "instance2"
    }
}

// Delte Instances
instance1.delete().onSuccess {
    // Invocation should fail after successful deletion
    instance1 {}
}
```

### Persistence

An `ingredient` is a variable that can be used just like any other variable in Kotlin, with the important difference that it only has a value in the context of an `instance`, and its value is automatically persisted.

```Kotlin
var myIngredient by ingredient(initialValue=0)

instance1 {
    println(myIngredient) // <- prints 0
}

println(myIngredient) // <- throws Exception because a only has a value in regard to a specific Instance
```

You can configure the backend for the persistence mechanism. Choose between `InMemory` and `Redis`, with `InMemory` being the default.

```Kotlin
Persistence.configure(
    PersistenceConfig.Redis(
        host = "localhost"
        port = mappedPort,
    )
)
```

An `ingredient` can be of any type as long as that type supports `kotlinx-serialization`.

```Kotlin
@Serializable
data class MyData(
    val a: Int,
    val b: Int,
)

var myIngredient by ingredient<MyData>() // <- works!
```

Because Katalyst is reactive, updating an `ingredient` might not have an immediate effect. Use `awaitIdle` to ensure all pending updates and reactions have been executed.

```Kotlin
var myIngredient by ingredient(initialValue=0)

instance1 {
    println(myIngredient) // <- prints 0
    myIngredient = 1
    println(myIngredient) // <- might still print 0
    awaitIdle()
    println(myIngredient) // <- will definitely print 1
}
```

When creating an `instance`, you define its time-to-live. This time is refreshed whenever the `instance` is used. Once the `instance` has expired, the persistence backend might not remove it immediately, but it will eventually be deleted. You can also delete an `instance` manually, which will have an immediate effect.

```Kotlin
val instance1 = createInstance("instance1", expiresAfter = 10.minutes).getOrThrow()

instance1 {
    // ...
} // invocation resets the 10.minutes

instance1.delete() // deleted immediately
```

Each `instance` is created with a unique `identifier: string`. Use `findInstances(pattern: String)` to get an `iterator` over all `instances` whose identifier matches the pattern. A pattern is a string with optional wildcard (*) characters.

```Kotlin
createInstance("cart-1").getOrThrow()
createInstance("cart-2").getOrThrow()

createInstance("user-1").getOrThrow()

findInstances("cart-*") // <- returns iterator over cart-1 and cart-2
```

### Reactivity

You might have multiple places in your code that update a certain `ingredient` and also the need to react to this state change in some unified way, no matter where the update ultimately came from. A `reaction` allows you to define this unified state change handler in a decoupled manner, potentially improving code quality.

A `reaction` will execute each time any `ingredient` it **reads** is updated. A `reaction` can also update an `ingredient`, triggering other `reactions` in turn (potential for loops).

```Kotlin
val budget = ingredient(initialValue=100)

val monitorBudgetReaction = reaction("monitor budget") {
    if (budget < 0) {
        sendWarningMail()
    }
}

val instance1 = createInstance("account1", setOf(monitorBudgetReaction)).getOrThrow()

instance1 {
    budget = budget - 110 // [100 - 110 = -10 < 0] will cause sendWarningMail() to execute
}
```

Katalyst maintains a queue of updates and reactions to execute. When an `ingredient` is written to, this update is appended to the queue. Then, all `reactions` that read the `ingredient` are appended behind it (in no specific order). If any of these `reactions` write to the same `ingredient` again, this update will also be appended (behind the already queued reactions), and the same `reactions` from before will be queued yet again (meaning the same reaction might appear in the queue multiple times). This keeps updates in order, makes sure no reactions are stepped over, and keeps things easy to reason about.

```Kotlin
val budget = ingredient(initialValue=100)

val monitorBudgetReaction = reaction("monitor budget") {
    if (budget < 0) {
        sendWarningMail()
    }
}

val instance1 = createInstance("account1", setOf(monitorBudgetReaction)).getOrThrow()

instance1 {
    budget = budget + 100
    budget = budget - 300
    budget = budget + 100
    awaitAll() // reaction ran (3 times) for each intermediate budget state (200, -100, 0)
}
```

## Examples

[Basic Example using InMemory Persistance](./src/main/kotlin/examples/quickstart/example.kt)

[Shoping Cart API using Redis Testcontainer Persitance](./src/main/kotlin/examples/shoppingCart/example.kt)

## Advanced

* How is state ownership managed in distibuted systems (Locking, local instance cache, value proxies, reference counting)
* How to handle long retries
* Errors Handling as Results

Comming soon..
