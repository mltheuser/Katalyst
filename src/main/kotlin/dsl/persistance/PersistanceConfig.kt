package dsl.persistance

sealed class PersistenceConfig {
    data object InMemory : PersistenceConfig()

    data class Redis(
        val host: String = "localhost",
        val port: Int = 6379,
        val database: Int = 0,
        val password: String? = null,
        val connectionTimeoutMillis: Long = 30000, // 30 seconds
        val commandTimeoutMillis: Long = 3000, // 3 seconds
        val lockLeaseTimeMillis: Long = 300000 // 5 minutes
    ) : PersistenceConfig()
}

object Persistence {
    @Volatile // Ensure visibility across threads
    private var _store: LockableKeyValueStore = InMemoryLockableKeyValueStore() // Start with in-memory default

    @Volatile private var storeAccessed: Boolean =
        false // Tracks if store has been accessed

    // Public accessor
    val store: LockableKeyValueStore
        get() {
            storeAccessed = true
            return _store // Return the currently configured store
        }

    @Synchronized // Ensures thread-safe configuration process
    fun configure(config: PersistenceConfig) {
        if (storeAccessed) {
            throw RuntimeException("ERROR: Tried to reconfigure persistence backend after it has already been accessed.")
        }

        // Create and set the new store
        _store = when (config) {
            is PersistenceConfig.InMemory -> InMemoryLockableKeyValueStore()
            is PersistenceConfig.Redis -> RedisLockableKeyValueStore(config)
        }

        println("INFO: Persistence configured to use ${config::class.simpleName}.")
    }
}