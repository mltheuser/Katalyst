package dsl.persistance

import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

data class LockHandle(internal val id: String)

interface LockableKeyValueStore {

    suspend fun lock(key: String, timeout: Duration = 5.seconds): Result<LockHandle>

    suspend fun unlock(key: String, lockHandle: LockHandle): Result<Unit>

    suspend fun exists(key: String): Result<Boolean>

    suspend fun get(key: String, lockHandle: LockHandle): Result<String>

    suspend fun set(key: String, value: String, lockHandle: LockHandle, expiresAfter: Duration): Result<Unit>

    suspend fun findKeysByPattern(pattern: String): Result<Iterator<String>>

    suspend fun delete(key: String, lockHandle: LockHandle): Result<Unit>
}