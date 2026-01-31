//===----------------------------------------------------------------------===//
//                         DuckDB
//
// mysql_connection_pool.hpp
//
//===----------------------------------------------------------------------===//
//
// Connection Pool for MySQL connections with the following guarantees:
//
// OWNERSHIP MODEL:
// - MySQLCatalog owns a shared_ptr<MySQLConnectionPool>
// - PooledConnection holds a shared_ptr to keep the pool alive during return
// - Connections are pinned to transactions until Commit/Rollback
//
// THREAD SAFETY:
// - Acquire() and Return() are thread-safe
// - Multiple threads can acquire connections concurrently up to max_connections
// - Pool uses mutex + condition_variable for synchronization
//
// CONNECTION LIFECYCLE:
// - Connections are created lazily on first Acquire()
// - Health checked with mysql_ping() before reuse
// - State reset with mysql_reset_connection() on return (drops temp tables, prepared stmts)
// - Connections discarded if health check or reset fails
//
// SHUTDOWN:
// - Call Shutdown() before destroying the pool to wake waiting threads
// - Destructor will clear remaining connections
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "mysql_connection.hpp"
#include "storage/federation/cost_model.hpp"

#include <atomic>
#include <condition_variable>
#include <deque>
#include <thread>

namespace duckdb {

class MySQLConnectionPool;

struct ThreadLocalConnectionCache {
	unique_ptr<MySQLConnection> connection;
	MySQLConnectionPool *owner = nullptr;
	bool available = false;
	std::thread::id thread_id;

	ThreadLocalConnectionCache() : thread_id(std::this_thread::get_id()) {
	}

	~ThreadLocalConnectionCache();

	void Clear();
};

class PooledConnection {
public:
	PooledConnection();
	PooledConnection(std::shared_ptr<MySQLConnectionPool> pool, unique_ptr<MySQLConnection> connection);
	~PooledConnection() noexcept;

	PooledConnection(const PooledConnection &) = delete;
	PooledConnection &operator=(const PooledConnection &) = delete;

	PooledConnection(PooledConnection &&other) noexcept;
	PooledConnection &operator=(PooledConnection &&other) noexcept;

	MySQLConnection &GetConnection();
	MySQLConnection *operator->();
	explicit operator bool() const;

	void Invalidate();

private:
	void ReturnToPool() noexcept;

	std::shared_ptr<MySQLConnectionPool> pool;
	unique_ptr<MySQLConnection> connection;
	bool valid = false;
};

class MySQLConnectionPool : public std::enable_shared_from_this<MySQLConnectionPool> {
public:
	static constexpr idx_t DEFAULT_POOL_SIZE = 4;
	static constexpr idx_t DEFAULT_POOL_TIMEOUT_MS = 30000;

	MySQLConnectionPool(string connection_string, string attach_path, MySQLTypeConfig type_config,
	                    idx_t max_connections = DEFAULT_POOL_SIZE, idx_t timeout_ms = DEFAULT_POOL_TIMEOUT_MS);
	~MySQLConnectionPool();

	[[nodiscard]] PooledConnection Acquire();
	void Return(unique_ptr<MySQLConnection> conn);
	void Discard();
	void Shutdown();

	idx_t GetMaxConnections() const;
	idx_t GetAvailableConnections() const;
	idx_t GetTotalConnections() const;
	bool IsShutdown() const;
	void UpdateTypeConfig(MySQLTypeConfig new_config);

	NetworkCalibration GetNetworkCalibration() const;
	void EnsureCalibrated(MySQLConnection &conn);
	void SetNetworkCompression(bool enabled, double ratio = NetworkCalibration::DEFAULT_COMPRESSION_RATIO);

	idx_t GetThreadLocalCacheHits() const;
	idx_t GetThreadLocalCacheMisses() const;
	void SetThreadLocalCacheEnabled(bool enabled);
	bool IsThreadLocalCacheEnabled() const;

private:
	friend struct ThreadLocalConnectionCache;

	void CalibrateNetwork(MySQLConnection &conn);
	unique_ptr<MySQLConnection> CreateConnection();
	bool IsConnectionHealthy(MySQLConnection &conn);
	void ResetConnectionState(MySQLConnection &conn);

	unique_ptr<MySQLConnection> TryAcquireFromThreadLocal();
	bool TryReturnToThreadLocal(unique_ptr<MySQLConnection> &conn);
	void ReturnFromThreadLocalCache(unique_ptr<MySQLConnection> conn);

	string connection_string;
	string attach_path;
	MySQLTypeConfig type_config;
	idx_t max_connections;
	idx_t timeout_ms;

	mutable mutex pool_lock;
	std::condition_variable pool_cv;
	std::deque<unique_ptr<MySQLConnection>> available;
	idx_t total_connections = 0;
	bool shutdown = false;
	NetworkCalibration network_calibration;

	std::atomic<bool> thread_local_cache_enabled {true};
	std::atomic<idx_t> thread_local_cache_hits {0};
	std::atomic<idx_t> thread_local_cache_misses {0};
};

} // namespace duckdb
