**How to Run and Test:**

1.  Make sure you have the Ktor dependencies added to your project.
2.  Run the `main` function. The server will start, usually on `http://0.0.0.0:8080`.
3.  Use a tool like `curl`, Postman, or Insomnia to interact with the endpoints:

    *   **Create a Cart:**
        ```bash
        curl -X POST http://localhost:8080/cart/create
        ```
        *   This will return JSON like: `{"cartId":"some-uuid-string"}`. Copy this ID.

    *   **Add Items (replace `YOUR_CART_ID`):**
        ```bash
        # Add 'apple'
        curl -X POST -H "Content-Type: application/json" -d '{"itemName": "apple"}' http://localhost:8080/cart/YOUR_CART_ID/add

        # Add 'banana'
        curl -X POST -H "Content-Type: application/json" -d '{"itemName": "banana"}' http://localhost:8080/cart/YOUR_CART_ID/add
        ```
        *   Check the server console logs. You should see messages about adding items and the "Log Cart Changes" Reaction running.

    *   **Submit the Order (replace `YOUR_CART_ID`):**
        ```bash
        curl -X POST http://localhost:8080/cart/YOUR_CART_ID/submit
        ```
        *   The server console will print the "Receipt".
        *   The response will contain a confirmation message and the list of items.
        *   Check the console logs for the status changing to `SUBMITTED`.

    *   **Try Adding After Submission (replace `YOUR_CART_ID`):**
        ```bash
        curl -X POST -H "Content-Type: application/json" -d '{"itemName": "orange"}' http://localhost:8080/cart/YOUR_CART_ID/add
        ```
        *   This should now return a `400 Bad Request` response with the message "Cart 'YOUR_CART_ID' has already been submitted."
        * 
    *  **Fetch all submitted ordered items**
        ```bash
        curl -X GET -H "Content-Type: application/json" http://localhost:8080/cart/summary
        ```

This example demonstrates how each cart (`RecipeInstance`) maintains its isolated state (`items`, `status`) via the `Store` and `PropertyAccess` delegates. The Ktor handlers interact with the correct instance using the `cartInstance { ... }` scope, ensuring thread safety and context propagation provided by the recipe framework.