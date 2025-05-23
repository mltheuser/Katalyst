import com.redis.testcontainers.RedisContainer
import dsl.components.Store
import dsl.components.createInstance
import dsl.components.findInstance
import dsl.components.interaction
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
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.minutes


// --- Shopping Cart Application ---

// 1. Define Cart State using the Store
enum class CartStatus { OPEN, PRIORITY, SUBMITTED }

object CartStore : Store() {
    // Ingredient holding the list of items in the cart for a specific instance
    var items by ingredient<List<String>>(emptyList())

    // Ingredient holding the status of the cart for a specific instance
    var status by ingredient(CartStatus.OPEN)
}

// 2. Define the Recipe (Interactions reacting to state changes)
val cartRecipe = setOf(
    // Example: An interaction that logs when items are added/removed or status changes.
    interaction("Log Cart Changes") {
        val currentItems = CartStore.items // Dependency on items
        val currentStatus = CartStore.status // Dependency on status
        println("[$instanceId] Cart State Update: Status=$currentStatus, Items=$currentItems")
        // This interaction doesn't write (target) anything, just logs.
    },

    interaction("Monitor Cart Size") {
        val currentItems = CartStore.items // Dependency on items
        if (currentItems.size > 3) {
            CartStore.status = CartStatus.PRIORITY

            println("[$instanceId] Cart has enough items to reach priority: Items=$currentItems")
        }
    },
    // We could add interactions triggered specifically by CartStatus.SUBMITTED
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

    // val mappedPort = startRedisContainer()

    Persistence.configure(PersistenceConfig.InMemory)

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
            post("/cart/create") { // The block here is already a suspend lambda
                val cartId = UUID.randomUUID().toString()
                println("Received request to create cart. Generating ID: $cartId")

                // --- Directly call suspend functions ---
                createInstance(cartId, cartRecipe, expiresAfter = 3.minutes)

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

                val cartInstance = cartId.findInstance().getOrThrow()

                var addSuccessful = false
                // Execute modifications within the specific cart's instance context
                cartInstance { // Sets RecipeContext for this block
                    // Check if cart is already submitted before adding
                    if (CartStore.status == CartStatus.SUBMITTED) {
                        println("[$cartId] Attempted to add item to already submitted cart.")
                        call.respond(HttpStatusCode.BadRequest, "Cart '$cartId' has already been submitted.")
                        // No need to awaitIdle here as no state was changed
                    } else {
                        // Read current items, add new one, and set the ingredient
                        val currentItems = CartStore.items
                        println("[$cartId] Current items: $currentItems. Adding '$itemName'")
                        CartStore.items = currentItems + itemName // Setting triggers notifyUpdate -> runs interactions

                        // Wait for the update (and logging interaction) to complete
                        awaitIdle()
                        println("[$cartId] Item '$itemName' added successfully.")
                        addSuccessful = true // Mark success for response after the block
                    }
                } // RecipeContext is reset after this block

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

                val cartInstance = cartId.findInstance().getOrThrow()

                println("[$cartId] Received request to submit order.")

                var finalItems: List<String> = emptyList()
                var alreadySubmitted = false

                // Execute submission logic within the instance context
                cartInstance {
                    if (CartStore.status == CartStatus.SUBMITTED) {
                        println("[$cartId] Cart already submitted.")
                        finalItems = CartStore.items // Get items anyway for response
                        alreadySubmitted = true
                        // Don't change state, maybe don't even awaitIdle
                    } else {
                        println("[$cartId] Submitting order...")
                        finalItems = CartStore.items // Get items before changing status
                        CartStore.status = CartStatus.SUBMITTED // Setting triggers notifyUpdate

                        // --- Simulate Receipt Generation ---
                        println("[$cartId] --- RECEIPT ---")
                        println("[$cartId] Cart ID: $instanceId")
                        println("[$cartId] Final Items (${finalItems.size}):")
                        finalItems.forEachIndexed { index, item -> println("[$cartId]   ${index + 1}. $item") }
                        println("[$cartId] Status: ${CartStore.status}") // Should print SUBMITTED
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