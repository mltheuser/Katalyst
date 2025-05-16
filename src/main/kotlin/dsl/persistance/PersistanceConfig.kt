package dsl.persistance

sealed class PersistenceConfig {
    data object InMemory : PersistenceConfig()

    data class Redis(
        val host: String = "localhost",
        val port: Int = 6379,
        val database: Int = 0,
        // Add more as necessary
    ) : PersistenceConfig()
}

object Persistence {
    @Volatile // Ensure visibility across threads
    private var _store: LockableKeyValueStore = InMemoryLockableKeyValueStore() // Start with in-memory default

    // Public accessor
    // TODO: should make note of when first accessed. So that we can throw warning when reconfigured after accessed.
    val store: LockableKeyValueStore get() = _store // Return the currently configured store

    /**
     * Configures the persistence layer for the DSL.
     * Must be called *once* before any instances are created or accessed,
     * typically at application startup.
     *
     * @param config The desired persistence configuration.
     * @throws IllegalStateException if persistence has already been configured.
     */
    fun configure(config: PersistenceConfig) {
        // TODO
    }
}