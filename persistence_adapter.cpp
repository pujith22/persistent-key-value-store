#include "persistence_adapter.h"

#include <iostream>
#include <sstream>
#include <libpq-fe.h>
#include <vector>
#include "nlohmann/json.hpp"

#include <queue>
#include <thread>
#include <future>
#include <condition_variable>
#include <atomic>

// Implementation of PersistenceAdapter using libpq (PostgreSQL C client)

struct PersistenceAdapter::Impl {
    PGconn* conn{nullptr};
    bool prepared{false};
    // connection pool: vector of PGconn* (one per connection)
    std::vector<PGconn*> pool_conns;
    std::mutex pool_mtx;
    std::condition_variable pool_cv;
    std::queue<PGconn*> free_conns;

    // worker thread pool for async work
    std::vector<std::thread> workers;
    std::queue<std::function<void()>> tasks;
    std::mutex tasks_mtx;
    std::condition_variable tasks_cv;
    std::atomic<bool> stop_workers{false};
    std::atomic<int> dropped_conns{0};
    std::atomic<int> total_conn_creates{0};
    std::atomic<int> total_conn_create_failures{0};
};

static std::string to_string_int(int v) {
    return std::to_string(v);
}

PersistenceAdapter::PersistenceAdapter(const std::string &conninfo)
    : p_(std::make_unique<Impl>())
{
    // Create a single 'master' connection used for initial prepare (if pool size > 1, others will be prepared lazily)
    p_->conn = PQconnectdb(conninfo.c_str());
    if (PQstatus(p_->conn) != CONNECTION_OK) {
        std::string err = PQerrorMessage(p_->conn) ? PQerrorMessage(p_->conn) : "unknown error";
        if (p_->conn) { PQfinish(p_->conn); p_->conn = nullptr; }
        throw std::runtime_error("Failed to open database connection: " + err);
    }

    // Prepare statements for better performance
    const char* prep_insert =
        "INSERT INTO kv_store (key, value) VALUES ($1::int, $2::text) "
        "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, created_at = now();";
    const char* prep_delete = "DELETE FROM kv_store WHERE key = $1::int;";
    const char* prep_select = "SELECT value FROM kv_store WHERE key = $1::int;";
    const char* prep_update = "UPDATE kv_store SET value = $2::text, created_at = now() WHERE key = $1::int;";

    PGresult* r1 = PQprepare(p_->conn, "kv_insert", prep_insert, 2, nullptr);
    if (PQresultStatus(r1) != PGRES_COMMAND_OK) {
        std::string err = PQerrorMessage(p_->conn);
        PQclear(r1);
        throw std::runtime_error("Prepare kv_insert failed: " + err);
    }
    PQclear(r1);
    PGresult* r2 = PQprepare(p_->conn, "kv_delete", prep_delete, 1, nullptr);
    if (PQresultStatus(r2) != PGRES_COMMAND_OK) {
        std::string err = PQerrorMessage(p_->conn);
        PQclear(r2);
        throw std::runtime_error("Prepare kv_delete failed: " + err);
    }
    PQclear(r2);
    PGresult* r3 = PQprepare(p_->conn, "kv_select", prep_select, 1, nullptr);
    if (PQresultStatus(r3) != PGRES_COMMAND_OK) {
        std::string err = PQerrorMessage(p_->conn);
        PQclear(r3);
        throw std::runtime_error("Prepare kv_select failed: " + err);
    }
    PQclear(r3);
    PGresult* r4 = PQprepare(p_->conn, "kv_update", prep_update, 2, nullptr);
    if (PQresultStatus(r4) != PGRES_COMMAND_OK) {
        std::string err = PQerrorMessage(p_->conn);
        PQclear(r4);
        throw std::runtime_error("Prepare kv_update failed: " + err);
    }
    PQclear(r4);
    p_->prepared = true;

    // Connection pool size from env or default
    int pool_size = 8;
    const char* pool_env = std::getenv("DB_POOL_SIZE");
    if (pool_env) {
        try { pool_size = std::stoi(pool_env); } catch(...) { }
        if (pool_size <= 0) pool_size = 1;
    }

    // initialize pool connections (we'll reuse the already-created p_->conn as first)
    p_->pool_conns.push_back(p_->conn);
    for (int i = 1; i < pool_size; ++i) {
        PGconn* c = PQconnectdb(conninfo.c_str());
        if (PQstatus(c) != CONNECTION_OK) {
            PQfinish(c);
            p_->total_conn_create_failures.fetch_add(1);
            // on failure, continue with fewer connections
            continue;
        }
        p_->pool_conns.push_back(c);
        p_->total_conn_creates.fetch_add(1);
    }
    // Ensure prepared statements exist on each pooled connection. If prepare fails for a connection,
    // close and drop that connection to avoid runtime errors like "prepared statement ... does not exist".
    std::vector<PGconn*> good_conns;
    for (auto cptr : p_->pool_conns) {
        bool ok = true;
        // If this is the original master connection and we already prepared earlier, skip re-preparing
        if (cptr == p_->conn && p_->prepared) {
            good_conns.push_back(cptr);
            continue;
        }
        PGresult* r1 = PQprepare(cptr, "kv_insert", prep_insert, 2, nullptr);
        if (PQresultStatus(r1) != PGRES_COMMAND_OK) ok = false; PQclear(r1);
        PGresult* r2 = PQprepare(cptr, "kv_delete", prep_delete, 1, nullptr);
        if (PQresultStatus(r2) != PGRES_COMMAND_OK) ok = false; PQclear(r2);
        PGresult* r3 = PQprepare(cptr, "kv_select", prep_select, 1, nullptr);
        if (PQresultStatus(r3) != PGRES_COMMAND_OK) ok = false; PQclear(r3);
        PGresult* r4 = PQprepare(cptr, "kv_update", prep_update, 2, nullptr);
        if (PQresultStatus(r4) != PGRES_COMMAND_OK) ok = false; PQclear(r4);
        if (ok) good_conns.push_back(cptr);
        else {
            std::cerr << "Warning: dropping pool connection due to prepare failure: " << PQerrorMessage(cptr);
            PQfinish(cptr);
            p_->dropped_conns.fetch_add(1);
            p_->total_conn_create_failures.fetch_add(1);
        }
    }
    p_->pool_conns.swap(good_conns);
    {
        std::lock_guard<std::mutex> lg(p_->pool_mtx);
        for (auto cptr : p_->pool_conns) p_->free_conns.push(cptr);
    }

    // start worker threads
    int workers_n = 4;
    const char* workers_env = std::getenv("DB_WORKER_THREADS");
    if (workers_env) {
        try { workers_n = std::stoi(workers_env); } catch(...) {}
        if (workers_n <= 0) workers_n = 1;
    }
    for (int i = 0; i < workers_n; ++i) {
        p_->workers.emplace_back([this]() {
            while (!p_->stop_workers.load()) {
                std::function<void()> task;
                {
                    std::unique_lock<std::mutex> lk(p_->tasks_mtx);
                    p_->tasks_cv.wait(lk, [this](){ return p_->stop_workers.load() || !p_->tasks.empty(); });
                    if (p_->stop_workers.load() && p_->tasks.empty()) return;
                    task = std::move(p_->tasks.front()); p_->tasks.pop();
                }
                try { task(); } catch(...) {}
            }
        });
    }
}

int PersistenceAdapter::droppedPoolConnections() const {
    if (!p_) return 0;
    return p_->dropped_conns.load();
}

nlohmann::json PersistenceAdapter::poolMetrics() const {
    nlohmann::json j;
    if (!p_) return j;
    std::lock_guard<std::mutex> lg(p_->pool_mtx);
    j["pool_size"] = static_cast<int>(p_->pool_conns.size());
    j["free_conns"] = static_cast<int>(p_->free_conns.size());
    j["dropped_conns"] = p_->dropped_conns.load();
    j["total_conn_creates"] = p_->total_conn_creates.load();
    j["total_conn_create_failures"] = p_->total_conn_create_failures.load();
    return j;
}

PersistenceAdapter::~PersistenceAdapter()
{
    if (!p_) return;
    // stop workers
    p_->stop_workers.store(true);
    p_->tasks_cv.notify_all();
    for (auto &t : p_->workers) if (t.joinable()) t.join();

    // close pool connections
    for (auto c : p_->pool_conns) {
        if (c) PQfinish(c);
    }
    p_->pool_conns.clear();
    p_->conn = nullptr;
}

bool PersistenceAdapter::insert(int key, const std::string &value)
{
    if (!p_) return false;
    // borrow connection
    PGconn* conn = nullptr;
    {
        std::unique_lock<std::mutex> lk(p_->pool_mtx);
        p_->pool_cv.wait(lk, [this](){ return !p_->free_conns.empty(); });
        conn = p_->free_conns.front(); p_->free_conns.pop();
    }
    bool ok = false;
    try {
        std::string keyStr = to_string_int(key);
        const char* params[2] = { keyStr.c_str(), value.c_str() };
        PGresult* res = PQexecPrepared(conn, "kv_insert", 2, params, nullptr, nullptr, 0);
        ok = PQresultStatus(res) == PGRES_COMMAND_OK;
        if (!ok) std::cerr << "insert() error: " << PQerrorMessage(conn);
        PQclear(res);
    } catch(...) { ok = false; }
    // return conn
    {
        std::lock_guard<std::mutex> lg(p_->pool_mtx);
        p_->free_conns.push(conn);
    }
    p_->pool_cv.notify_one();
    return ok;
}

bool PersistenceAdapter::update(int key, const std::string &value)
{
    if (!p_) return false;
    PGconn* conn = nullptr;
    {
        std::unique_lock<std::mutex> lk(p_->pool_mtx);
        p_->pool_cv.wait(lk, [this](){ return !p_->free_conns.empty(); });
        conn = p_->free_conns.front(); p_->free_conns.pop();
    }
    bool ok = false;
    int affected = 0;
    try {
        std::string keyStr = to_string_int(key);
        const char* params[2] = { keyStr.c_str(), value.c_str() };
        PGresult* res = PQexecPrepared(conn, "kv_update", 2, params, nullptr, nullptr, 0);
        ok = PQresultStatus(res) == PGRES_COMMAND_OK;
        if (ok) {
            const char* tuples = PQcmdTuples(res);
            affected = (tuples && *tuples) ? std::stoi(tuples) : 0;
        } else {
            std::cerr << "update() error: " << PQerrorMessage(conn);
        }
        PQclear(res);
    } catch(...) { ok = false; }
    {
        std::lock_guard<std::mutex> lg(p_->pool_mtx);
        p_->free_conns.push(conn);
    }
    p_->pool_cv.notify_one();
    return affected > 0;
}

bool PersistenceAdapter::remove(int key)
{
    if (!p_) return false;
    PGconn* conn = nullptr;
    {
        std::unique_lock<std::mutex> lk(p_->pool_mtx);
        p_->pool_cv.wait(lk, [this](){ return !p_->free_conns.empty(); });
        conn = p_->free_conns.front(); p_->free_conns.pop();
    }
    bool ok = false; int affected = 0;
    try {
        std::string keyStr = to_string_int(key);
        const char* params[1] = { keyStr.c_str() };
        PGresult* res = PQexecPrepared(conn, "kv_delete", 1, params, nullptr, nullptr, 0);
        ok = PQresultStatus(res) == PGRES_COMMAND_OK;
        if (ok) {
            const char* tuples = PQcmdTuples(res);
            affected = (tuples && *tuples) ? std::stoi(tuples) : 0;
        } else {
            std::cerr << "remove() error: " << PQerrorMessage(conn);
        }
        PQclear(res);
    } catch(...) { ok = false; }
    {
        std::lock_guard<std::mutex> lg(p_->pool_mtx);
        p_->free_conns.push(conn);
    }
    p_->pool_cv.notify_one();
    return affected > 0;
}

std::unique_ptr<std::string> PersistenceAdapter::get(int key)
{
    if (!p_) return nullptr;
    // borrow a connection from pool
    PGconn* conn = nullptr;
    {
        std::unique_lock<std::mutex> lk(p_->pool_mtx);
        p_->pool_cv.wait(lk, [this](){ return !p_->free_conns.empty(); });
        conn = p_->free_conns.front(); p_->free_conns.pop();
    }

    std::unique_ptr<std::string> out;
    try {
        std::string keyStr = to_string_int(key);
        const char* params[1] = { keyStr.c_str() };
        PGresult* res = PQexecPrepared(conn, "kv_select", 1, params, nullptr, nullptr, 0);
        if (PQresultStatus(res) == PGRES_TUPLES_OK) {
            if (PQntuples(res) == 1 && PQnfields(res) == 1) {
                char* val = PQgetvalue(res, 0, 0);
                if (val) out = std::make_unique<std::string>(val);
            }
        } else {
            std::cerr << "get() error: " << PQerrorMessage(conn);
        }
        PQclear(res);
    } catch(...) {
        // swallow; return null
    }

    // return connection to pool
    {
        std::lock_guard<std::mutex> lg(p_->pool_mtx);
        p_->free_conns.push(conn);
    }
    p_->pool_cv.notify_one();
    return out;
}

PersistenceAdapter::TxResult PersistenceAdapter::runTransaction(const std::vector<Operation>& ops, TxMode mode)
{
    TxResult result{true, {}};
    if (!p_ || !p_->conn) {
        result.success = false;
        result.failures.push_back({Operation{OpType::Insert, 0, ""}, "no connection"});
        return result;
    }
    // borrow a connection for the transaction
    PGconn* conn = nullptr;
    {
        std::unique_lock<std::mutex> lk(p_->pool_mtx);
        p_->pool_cv.wait(lk, [this](){ return !p_->free_conns.empty(); });
        conn = p_->free_conns.front(); p_->free_conns.pop();
    }

    auto exec_simple = [&](const char* sql) -> bool {
        PGresult* r = PQexec(conn, sql);
        bool ok = PQresultStatus(r) == PGRES_COMMAND_OK;
        if (!ok) std::cerr << "txn error: " << PQerrorMessage(conn);
        PQclear(r);
        return ok;
    };

    if (!exec_simple("BEGIN")) {
        result.success = false;
        result.failures.push_back({Operation{OpType::Insert, 0, ""}, "BEGIN failed"});
        return result;
    }

    int idx = 0;
    for (const auto& op : ops) {
        ++idx;
        auto fail_this = [&](const std::string& msg) {
            result.failures.push_back({op, msg});
        };

        if (mode == TxMode::Silent) {
            // savepoint per-op
            std::ostringstream sp;
            sp << "SAVEPOINT sp_" << idx;
            if (!exec_simple(sp.str().c_str())) {
                fail_this("SAVEPOINT failed");
                continue; // best effort
            }

            auto release_sp = [&]() {
                std::ostringstream rel; rel << "RELEASE SAVEPOINT sp_" << idx; exec_simple(rel.str().c_str());
            };
            auto rollback_sp = [&]() {
                std::ostringstream rb; rb << "ROLLBACK TO SAVEPOINT sp_" << idx; exec_simple(rb.str().c_str());
                // release after rollback to clear savepoint
                std::ostringstream rel; rel << "RELEASE SAVEPOINT sp_" << idx; exec_simple(rel.str().c_str());
            };

            bool ok = false;
            int affected = 0;
            // execute op
            if (op.type == OpType::Insert) {
                std::string keyStr = std::to_string(op.key);
                const char* params[2] = { keyStr.c_str(), op.value.c_str() };
                PGresult* r = PQexecPrepared(p_->conn, "kv_insert", 2, params, nullptr, nullptr, 0);
                ok = PQresultStatus(r) == PGRES_COMMAND_OK;
                if (!ok) fail_this(PQerrorMessage(p_->conn));
                else {
                    const char* t = PQcmdTuples(r); affected = (t && *t) ? std::stoi(t) : 1; // treat insert as success even if 0 reported
                }
                PQclear(r);
            } else if (op.type == OpType::Update) {
                std::string keyStr = std::to_string(op.key);
                const char* params[2] = { keyStr.c_str(), op.value.c_str() };
                PGresult* r = PQexecPrepared(p_->conn, "kv_update", 2, params, nullptr, nullptr, 0);
                ok = PQresultStatus(r) == PGRES_COMMAND_OK;
                if (ok) {
                    const char* t = PQcmdTuples(r); affected = (t && *t) ? std::stoi(t) : 0;
                    if (affected == 0) { ok = false; fail_this("no rows affected"); }
                } else {
                    fail_this(PQerrorMessage(p_->conn));
                }
                PQclear(r);
            } else if (op.type == OpType::Remove) {
                std::string keyStr = std::to_string(op.key);
                const char* params[1] = { keyStr.c_str() };
                PGresult* r = PQexecPrepared(p_->conn, "kv_delete", 1, params, nullptr, nullptr, 0);
                ok = PQresultStatus(r) == PGRES_COMMAND_OK;
                if (ok) {
                    const char* t = PQcmdTuples(r); affected = (t && *t) ? std::stoi(t) : 0;
                    if (affected == 0) { ok = false; fail_this("no rows affected"); }
                } else {
                    fail_this(PQerrorMessage(p_->conn));
                }
                PQclear(r);
            }

            if (!ok) {
                rollback_sp();
                continue; // continue with next op
            } else {
                release_sp();
            }

        } else { // RollbackOnError
            bool ok = true; int affected = 0;
            if (op.type == OpType::Insert) {
                std::string keyStr = std::to_string(op.key);
                const char* params[2] = { keyStr.c_str(), op.value.c_str() };
                PGresult* r = PQexecPrepared(p_->conn, "kv_insert", 2, params, nullptr, nullptr, 0);
                ok = PQresultStatus(r) == PGRES_COMMAND_OK;
                if (!ok) fail_this(PQerrorMessage(p_->conn));
                PQclear(r);
            } else if (op.type == OpType::Update) {
                std::string keyStr = std::to_string(op.key);
                const char* params[2] = { keyStr.c_str(), op.value.c_str() };
                PGresult* r = PQexecPrepared(p_->conn, "kv_update", 2, params, nullptr, nullptr, 0);
                ok = PQresultStatus(r) == PGRES_COMMAND_OK;
                if (ok) {
                    const char* t = PQcmdTuples(r); affected = (t && *t) ? std::stoi(t) : 0;
                    if (affected == 0) { ok = false; fail_this("no rows affected"); }
                } else {
                    fail_this(PQerrorMessage(p_->conn));
                }
                PQclear(r);
            } else if (op.type == OpType::Remove) {
                std::string keyStr = std::to_string(op.key);
                const char* params[1] = { keyStr.c_str() };
                PGresult* r = PQexecPrepared(p_->conn, "kv_delete", 1, params, nullptr, nullptr, 0);
                ok = PQresultStatus(r) == PGRES_COMMAND_OK;
                if (ok) {
                    const char* t = PQcmdTuples(r); affected = (t && *t) ? std::stoi(t) : 0;
                    if (affected == 0) { ok = false; fail_this("no rows affected"); }
                } else {
                    fail_this(PQerrorMessage(p_->conn));
                }
                PQclear(r);
            }

            if (!ok) {
                exec_simple("ROLLBACK");
                result.success = false;
                return result;
            }
        }
    }

    if (!exec_simple("COMMIT")) {
        result.success = false;
        result.failures.push_back({Operation{OpType::Insert, 0, ""}, "COMMIT failed"});
    }

    // return transaction connection
    {
        std::lock_guard<std::mutex> lg(p_->pool_mtx);
        p_->free_conns.push(conn);
    }
    p_->pool_cv.notify_one();
    return result;
}

std::future<std::unique_ptr<std::string>> PersistenceAdapter::getAsync(int key) {
    if (!p_) return std::async(std::launch::deferred, [](){ return std::unique_ptr<std::string>(nullptr); });
    auto prom = std::make_shared<std::promise<std::unique_ptr<std::string>>>();
    auto fut = prom->get_future();
    {
        std::lock_guard<std::mutex> lg(p_->tasks_mtx);
        p_->tasks.emplace([this, key, prom]() {
            try {
                auto res = this->get(key);
                prom->set_value(std::move(res));
            } catch (...) { prom->set_value(nullptr); }
        });
    }
    p_->tasks_cv.notify_one();
    return fut;
}

std::future<nlohmann::json> PersistenceAdapter::runTransactionJsonAsync(const std::vector<Operation>& ops, TxMode mode) {
    if (!p_) return std::async(std::launch::deferred, [](){ return nlohmann::json(); });
    auto prom = std::make_shared<std::promise<nlohmann::json>>();
    auto fut = prom->get_future();
    {
        std::lock_guard<std::mutex> lg(p_->tasks_mtx);
        p_->tasks.emplace([this, ops, mode, prom]() {
            try {
                auto res = this->runTransactionJson(ops, mode);
                prom->set_value(std::move(res));
            } catch (...) { prom->set_value(nlohmann::json()); }
        });
    }
    p_->tasks_cv.notify_one();
    return fut;
}

nlohmann::json PersistenceAdapter::runTransactionJson(const std::vector<Operation>& ops, TxMode mode)
{
    nlohmann::json report;
    report["mode"] = (mode == TxMode::Silent) ? "silent" : "rollback";
    report["success"] = true;
    report["results"] = nlohmann::json::array();

    auto push_result = [&](OpType type, int key, const char* status, const std::string& error = std::string(), const nlohmann::json& value = nullptr) {
        nlohmann::json item;
        item["op"] = (type == OpType::Insert) ? "insert" : (type == OpType::Update) ? "update" : (type == OpType::Remove) ? "remove" : "get";
        item["key"] = key;
        item["status"] = status;
        if (!error.empty()) item["error"] = error;
        if (type == OpType::Get) item["value"] = value; // may be string or null
        report["results"].push_back(std::move(item));
    };

    if (!p_ || !p_->conn) {
        report["success"] = false;
        push_result(OpType::Insert, 0, "failed", "no connection");
        return report;
    }

    auto exec_simple = [&](const char* sql) -> bool {
        PGresult* r = PQexec(p_->conn, sql);
        bool ok = PQresultStatus(r) == PGRES_COMMAND_OK;
        if (!ok) std::cerr << "txn error: " << PQerrorMessage(p_->conn);
        PQclear(r);
        return ok;
    };

    if (!exec_simple("BEGIN")) {
        report["success"] = false;
        push_result(OpType::Insert, 0, "failed", "BEGIN failed");
        return report;
    }

    int idx = 0;
    for (const auto& op : ops) {
        ++idx;
        if (mode == TxMode::Silent) {
            // savepoint per op
            std::ostringstream sp; sp << "SAVEPOINT sp_" << idx;
            if (!exec_simple(sp.str().c_str())) { push_result(op.type, op.key, "failed", "SAVEPOINT failed"); continue; }
            auto release_sp = [&]() { std::ostringstream rel; rel << "RELEASE SAVEPOINT sp_" << idx; exec_simple(rel.str().c_str()); };
            auto rollback_sp = [&]() { std::ostringstream rb; rb << "ROLLBACK TO SAVEPOINT sp_" << idx; exec_simple(rb.str().c_str()); std::ostringstream rel; rel << "RELEASE SAVEPOINT sp_" << idx; exec_simple(rel.str().c_str()); };

            bool ok = true; int affected = 0;
            if (op.type == OpType::Insert) {
                std::string k = std::to_string(op.key);
                const char* params[2] = { k.c_str(), op.value.c_str() };
                PGresult* r = PQexecPrepared(p_->conn, "kv_insert", 2, params, nullptr, nullptr, 0);
                ok = PQresultStatus(r) == PGRES_COMMAND_OK;
                if (!ok) push_result(op.type, op.key, "failed", PQerrorMessage(p_->conn));
                else push_result(op.type, op.key, "ok");
                PQclear(r);
            } else if (op.type == OpType::Update) {
                std::string k = std::to_string(op.key);
                const char* params[2] = { k.c_str(), op.value.c_str() };
                PGresult* r = PQexecPrepared(p_->conn, "kv_update", 2, params, nullptr, nullptr, 0);
                ok = PQresultStatus(r) == PGRES_COMMAND_OK;
                if (ok) { const char* t = PQcmdTuples(r); affected = (t && *t) ? std::stoi(t) : 0; if (affected == 0) { ok = false; push_result(op.type, op.key, "failed", "no rows affected"); } }
                else { push_result(op.type, op.key, "failed", PQerrorMessage(p_->conn)); }
                if (ok) push_result(op.type, op.key, "ok");
                PQclear(r);
            } else if (op.type == OpType::Remove) {
                std::string k = std::to_string(op.key);
                const char* params[1] = { k.c_str() };
                PGresult* r = PQexecPrepared(p_->conn, "kv_delete", 1, params, nullptr, nullptr, 0);
                ok = PQresultStatus(r) == PGRES_COMMAND_OK;
                if (ok) { const char* t = PQcmdTuples(r); affected = (t && *t) ? std::stoi(t) : 0; if (affected == 0) { ok = false; push_result(op.type, op.key, "failed", "no rows affected"); } }
                else { push_result(op.type, op.key, "failed", PQerrorMessage(p_->conn)); }
                if (ok) push_result(op.type, op.key, "ok");
                PQclear(r);
            } else if (op.type == OpType::Get) {
                std::string k = std::to_string(op.key);
                const char* params[1] = { k.c_str() };
                PGresult* r = PQexecPrepared(p_->conn, "kv_select", 1, params, nullptr, nullptr, 0);
                ok = (PQresultStatus(r) == PGRES_TUPLES_OK);
                if (!ok) {
                    push_result(op.type, op.key, "failed", PQerrorMessage(p_->conn), nullptr);
                } else {
                    if (PQntuples(r) == 1 && PQnfields(r) == 1) {
                        char* val = PQgetvalue(r, 0, 0);
                        push_result(op.type, op.key, "ok", "", std::string(val ? val : ""));
                    } else {
                        // not found -> value null in silent mode
                        push_result(op.type, op.key, "ok", "", nullptr);
                    }
                }
                PQclear(r);
            }

            if (!ok) { rollback_sp(); } else { release_sp(); }

        } else { // RollbackOnError
            bool ok = true; int affected = 0;
            if (op.type == OpType::Insert) {
                std::string k = std::to_string(op.key);
                const char* params[2] = { k.c_str(), op.value.c_str() };
                PGresult* r = PQexecPrepared(p_->conn, "kv_insert", 2, params, nullptr, nullptr, 0);
                ok = PQresultStatus(r) == PGRES_COMMAND_OK;
                if (ok) push_result(op.type, op.key, "ok"); else push_result(op.type, op.key, "failed", PQerrorMessage(p_->conn));
                PQclear(r);
            } else if (op.type == OpType::Update) {
                std::string k = std::to_string(op.key);
                const char* params[2] = { k.c_str(), op.value.c_str() };
                PGresult* r = PQexecPrepared(p_->conn, "kv_update", 2, params, nullptr, nullptr, 0);
                ok = PQresultStatus(r) == PGRES_COMMAND_OK;
                if (ok) { const char* t = PQcmdTuples(r); affected = (t && *t) ? std::stoi(t) : 0; if (affected == 0) { ok = false; push_result(op.type, op.key, "failed", "no rows affected"); } }
                else { push_result(op.type, op.key, "failed", PQerrorMessage(p_->conn)); }
                if (ok) push_result(op.type, op.key, "ok");
                PQclear(r);
            } else if (op.type == OpType::Remove) {
                std::string k = std::to_string(op.key);
                const char* params[1] = { k.c_str() };
                PGresult* r = PQexecPrepared(p_->conn, "kv_delete", 1, params, nullptr, nullptr, 0);
                ok = PQresultStatus(r) == PGRES_COMMAND_OK;
                if (ok) { const char* t = PQcmdTuples(r); affected = (t && *t) ? std::stoi(t) : 0; if (affected == 0) { ok = false; push_result(op.type, op.key, "failed", "no rows affected"); } }
                else { push_result(op.type, op.key, "failed", PQerrorMessage(p_->conn)); }
                if (ok) push_result(op.type, op.key, "ok");
                PQclear(r);
            } else if (op.type == OpType::Get) {
                std::string k = std::to_string(op.key);
                const char* params[1] = { k.c_str() };
                PGresult* r = PQexecPrepared(p_->conn, "kv_select", 1, params, nullptr, nullptr, 0);
                ok = (PQresultStatus(r) == PGRES_TUPLES_OK);
                if (!ok) {
                    push_result(op.type, op.key, "failed", PQerrorMessage(p_->conn), nullptr);
                } else {
                    if (PQntuples(r) == 1 && PQnfields(r) == 1) {
                        char* val = PQgetvalue(r, 0, 0);
                        push_result(op.type, op.key, "ok", "", std::string(val ? val : ""));
                    } else {
                        // not found -> ok with null value
                        push_result(op.type, op.key, "ok", "", nullptr);
                    }
                }
                PQclear(r);
            }

            if (!ok) { exec_simple("ROLLBACK"); report["success"] = false; return report; }
        }
    }

    if (!exec_simple("COMMIT")) { report["success"] = false; push_result(OpType::Insert, 0, "failed", "COMMIT failed"); }
    return report;
}