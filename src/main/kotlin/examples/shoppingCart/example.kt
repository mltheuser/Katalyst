import com.redis.testcontainers.RedisContainer
import dsl.components.*
import dsl.persistance.Persistence
import dsl.persistance.PersistenceConfig
import io.ktor.http.*
import io.ktor.serialization.kotlinx.json.*
import io.ktor.server.application.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*
import io.ktor.server.plugins.contentnegotiation.*
import io.ktor.server.plugins.statuspages.*
import io.ktor.server.request.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import kotlinx.serialization.Serializable
import org.testcontainers.utility.DockerImageName
import java.util.*
import kotlin.time.Duration.Companion.minutes


// --- Shopping Cart Application ---

// 1. Define Cart State using Ingredients
enum class CartStatus { OPEN, PRIORITY, SUBMITTED }

object Cart {
    // Ingredient holding the list of items in the cart
    var items by ingredient<List<String>>(emptyList())

    // Ingredient holding the status of the cart
    var status by ingredient(CartStatus.OPEN)
}

// 2. Define the Reactions
val cartRecipe = setOf(
    // Example: An Reaction that logs when items are added/removed or status changes.
    reaction("Log Cart Changes") {
        val currentItems = Cart.items // Dependency on items
        val currentStatus = Cart.status // Dependency on status
        println("[$instanceId] Cart State Update: Status=$currentStatus, Items=$currentItems")
        // This Reaction doesn't write anything, just logs.
    },

    reaction("Monitor Cart Size") {
        val currentItems = Cart.items // Dependency on items
        if (currentItems.size > 3) {
            Cart.status = CartStatus.PRIORITY

            println("[$instanceId] Cart has enough items to reach priority: Items=$currentItems")
        }
    },
    // We could add Reactions triggered specifically by CartStatus.SUBMITTED
    // for example, to trigger external actions like sending notifications.
)

// --- Ktor Server Setup ---

// Data classes for request/response bodies (needs kotlinx-serialization)
@Serializable
data class CreateCartResponse(val cartId: String)

@Serializable
data class AddItemRequest(val itemName: String)

@Serializable
data class SubmitResponse(val message: String, val items: List<String>)


fun main() {

    val mappedPort = startRedisContainer()

    Persistence.configure(
        PersistenceConfig.Redis(
            port = mappedPort,
        )
    )

    println("Starting Ktor server for Shopping Cart...")
    embeddedServer(Netty, port = 8080, host = "0.0.0.0") {
        // Install JSON Content Negotiation
        install(ContentNegotiation) {
            json() // Uses kotlinx.serialization.json
        }
        install(StatusPages) {
            exception<Throwable> { call, cause ->
                println("Exception: $cause")
                call.respondText(text = "500: $cause", status = HttpStatusCode.InternalServerError)
            }
        }

        // Define Server Routing
        routing {
            // Endpoint to create a new shopping cart
            post("/cart/create") {
                val cartId = "cart-" + UUID.randomUUID().toString()
                println("Received request to create cart. Generating ID: $cartId")

                // --- Directly call suspend functions ---
                createInstance(cartId, cartRecipe, expiresAfter = 10.minutes)

                // call.respond is a suspend function, call it directly
                call.respond(HttpStatusCode.Created, CreateCartResponse(cartId))
                // This log should now execute after the response is successfully initiated
                println("Responded with new cart ID: $cartId")
                // --- End of direct calls ---
            }

            // Endpoint to add an item to a specific cart
            post("/cart/{cartId}/add") {
                val cartId = call.parameters["cartId"]
                if (cartId == null) {
                    call.respond(HttpStatusCode.BadRequest, "Missing cartId parameter")
                    return@post
                }

                // Deserialize request body to get the item name
                val request = try {
                    call.receive<AddItemRequest>()
                } catch (e: Exception) {
                    call.respond(HttpStatusCode.BadRequest, "Invalid request body: ${e.message}")
                    return@post
                }
                val itemName = request.itemName
                println("[$cartId] Received request to add item: '$itemName'")

                val cartInstance = cartId.lookupInstance().getOrThrow()

                var addSuccessful = false
                // Execute modifications within the specific cart's instance context
                cartInstance { // Sets RecipeContext for this block
                    // Check if cart is already submitted before adding
                    if (Cart.status == CartStatus.SUBMITTED) {
                        println("[$cartId] Attempted to add item to already submitted cart.")
                        call.respond(HttpStatusCode.BadRequest, "Cart '$cartId' has already been submitted.")
                        // No need to awaitIdle here as no state was changed
                    } else {
                        // Read current items, add new one, and set the ingredient
                        val currentItems = Cart.items
                        println("[$cartId] Current items: $currentItems. Adding '$itemName'")
                        Cart.items = currentItems + itemName // Setting triggers notifyUpdate -> runs Reactions

                        // Wait for the update (and logging Reaction) to complete
                        awaitIdle()
                        println("[$cartId] Item '$itemName' added successfully.")
                        addSuccessful = true // Mark success for response after the block
                    }
                }

                if (addSuccessful) {
                    call.respond(HttpStatusCode.OK, "Item '$itemName' added to cart '$cartId'.")
                }
                // If !addSuccessful, response was already sent inside the cartInstance block
            }

            // Endpoint to submit the order for a specific cart
            post("/cart/{cartId}/submit") {
                val cartId = call.parameters["cartId"]
                if (cartId == null) {
                    call.respond(HttpStatusCode.BadRequest, "Missing cartId parameter")
                    return@post
                }

                val cartInstance = cartId.lookupInstance().getOrThrow()

                println("[$cartId] Received request to submit order.")

                var finalItems: List<String> = emptyList()
                var alreadySubmitted = false

                // Execute submission logic within the instance context
                cartInstance {
                    if (Cart.status == CartStatus.SUBMITTED) {
                        println("[$cartId] Cart already submitted.")
                        finalItems = Cart.items // Get items anyway for response
                        alreadySubmitted = true
                        // Don't change state, don't even awaitIdle
                    } else {
                        println("[$cartId] Submitting order...")
                        finalItems = Cart.items // Get items before changing status
                        Cart.status = CartStatus.SUBMITTED // Setting triggers notifyUpdate

                        // --- Simulate Receipt Generation ---
                        println("[$cartId] --- RECEIPT ---")
                        println("[$cartId] Cart ID: $instanceId")
                        println("[$cartId] Final Items (${finalItems.size}):")
                        finalItems.forEachIndexed { index, item -> println("[$cartId]   ${index + 1}. $item") }
                        println("[$cartId] Status: ${Cart.status}") // Should print SUBMITTED
                        println("[$cartId] --- END RECEIPT ---")
                        // --- End Simulation ---

                        // Wait for the status update (and logging) to finish
                        awaitIdle()
                        println("[$cartId] Order submission processed.")
                    }
                } // Context reset

                if (alreadySubmitted) {
                    call.respond(HttpStatusCode.OK, SubmitResponse("Cart '$cartId' was already submitted.", finalItems))
                } else {
                    call.respond(
                        HttpStatusCode.OK,
                        SubmitResponse("Order for cart '$cartId' submitted successfully.", finalItems)
                    )
                }
            }

            // Endpoint to get a summary of all items across all active shopping carts
            get("/cart/summary") { // Changed to GET and a more appropriate path
                val itemsOrdered = mutableMapOf<String, Int>()

                // findInstances returns Result<List<RecipeInstanceContext<*>>>
                // We need to handle success and failure of this operation
                findInstances("cart-*").onSuccess { instances ->
                    instances.forEach { instance ->
                        // Execute code within the context of each found instance
                        // This lambda has access to CartStore specific to 'instance'
                        instance {
                            if (Cart.status != CartStatus.SUBMITTED) {
                                return@instance
                            }
                            val itemsInCart = Cart.items
                            println("[$instanceId] Processing items for summary: $itemsInCart")
                            itemsInCart.forEach { item ->
                                itemsOrdered[item] = itemsOrdered.getOrDefault(item, 0) + 1
                            }
                        }
                    }
                    // Successfully aggregated items
                    println("Cart Summary Report: $itemsOrdered")
                    call.respond(HttpStatusCode.OK, itemsOrdered) // kotlinx.serialization handles Map<String, Int>
                }.onFailure { error ->
                    // Log the error and respond appropriately
                    System.err.println("Error fetching cart instances for summary: ${error.message}")
                    error.printStackTrace() // For more detailed logging
                    call.respond(
                        HttpStatusCode.InternalServerError, "Failed to generate cart summary: ${error.message}"
                    )
                }
            }
        }
    }.start(wait = true) // Start server and block main thread
}

fun startRedisContainer(): Int {
    // 1. Setup and Start Redis Container
    val redisImageName = "redis:7-alpine"
    val exposedPort = 6379

    val redisContainer = RedisContainer(DockerImageName.parse(redisImageName)).apply {
        withExposedPorts(exposedPort)
    }

    // Use a shutdown hook or try-finally to ensure the container stops
    Runtime.getRuntime().addShutdownHook(Thread {
        println("Shutting down Redis container...")
        redisContainer.stop()
        println("Redis container stopped.")
    })

    println("Starting Redis container ($redisImageName)...")
    redisContainer.start()

    val mappedPort = redisContainer.getMappedPort(exposedPort)
    println("Redis container started on ${redisContainer.host}:${redisContainer.getMappedPort(exposedPort)}")

    return mappedPort!!
}