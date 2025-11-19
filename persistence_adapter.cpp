#include "persistence_adapter.h"

#include <iostream>
#include <sstream>
#include <libpq-fe.h>
#include <vector>
#include <mutex>
#include "nlohmann/json.hpp"

// Implementation of PersistenceAdapter using libpq (PostgreSQL C client)

struct PersistenceAdapter::Impl {
    PGconn* conn{nullptr};
    bool prepared{false};
    std::mutex db_mutex;
};

static std::string to_string_int(int v) {
    return std::to_string(v);
}

PersistenceAdapter::PersistenceAdapter(const std::string &conninfo)
    : p_(std::make_unique<Impl>())
{
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
}

PersistenceAdapter::~PersistenceAdapter()
{
    if (p_ && p_->conn) {
        PQfinish(p_->conn);
        p_->conn = nullptr;
    }
}

bool PersistenceAdapter::insert(int key, const std::string &value)
{
    if (!p_ || !p_->conn) return false;
    std::lock_guard<std::mutex> lock(p_->db_mutex);

    std::string keyStr = to_string_int(key);
    const char* params[2] = { keyStr.c_str(), value.c_str() };

    PGresult* res = PQexecPrepared(
        p_->conn,
        "kv_insert",
        2,
        params,
        nullptr,
        nullptr,
        0
    );

    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        std::cerr << "insert() error: " << PQerrorMessage(p_->conn);
        PQclear(res);
        return false;
    }

    PQclear(res);
    return true;
}

bool PersistenceAdapter::update(int key, const std::string &value)
{
    if (!p_ || !p_->conn) return false;
    std::lock_guard<std::mutex> lock(p_->db_mutex);

    std::string keyStr = to_string_int(key);
    const char* params[2] = { keyStr.c_str(), value.c_str() };

    PGresult* res = PQexecPrepared(
        p_->conn,
        "kv_update",
        2,
        params,
        nullptr,
        nullptr,
        0
    );

    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        std::cerr << "update() error: " << PQerrorMessage(p_->conn);
        PQclear(res);
        return false;
    }

    const char* tuples = PQcmdTuples(res);
    int affected = (tuples && *tuples) ? std::stoi(tuples) : 0;
    PQclear(res);
    return affected > 0;
}

bool PersistenceAdapter::remove(int key)
{
    if (!p_ || !p_->conn) return false;
    std::lock_guard<std::mutex> lock(p_->db_mutex);

    std::string keyStr = to_string_int(key);
    const char* params[1] = { keyStr.c_str() };

    PGresult* res = PQexecPrepared(
        p_->conn,
        "kv_delete",
        1,
        params,
        nullptr,
        nullptr,
        0
    );

    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        std::cerr << "remove() error: " << PQerrorMessage(p_->conn);
        PQclear(res);
        return false;
    }

    // number of rows affected
    const char* tuples = PQcmdTuples(res);
    int affected = (tuples && *tuples) ? std::stoi(tuples) : 0;
    PQclear(res);
    return affected > 0;
}

std::unique_ptr<std::string> PersistenceAdapter::get(int key)
{
    if (!p_ || !p_->conn) return nullptr;
    std::lock_guard<std::mutex> lock(p_->db_mutex);

    std::string keyStr = to_string_int(key);
    const char* params[1] = { keyStr.c_str() };

    PGresult* res = PQexecPrepared(
        p_->conn,
        "kv_select",
        1,
        params,
        nullptr,
        nullptr,
        0
    );

    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        std::cerr << "get() error: " << PQerrorMessage(p_->conn);
        PQclear(res);
        return nullptr;
    }

    std::unique_ptr<std::string> out;
    if (PQntuples(res) == 1 && PQnfields(res) == 1) {
        char* val = PQgetvalue(res, 0, 0);
        if (val) out = std::make_unique<std::string>(val);
    }
    PQclear(res);
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
    std::lock_guard<std::mutex> lock(p_->db_mutex);

    auto exec_simple = [&](const char* sql) -> bool {
        PGresult* r = PQexec(p_->conn, sql);
        bool ok = PQresultStatus(r) == PGRES_COMMAND_OK;
        if (!ok) std::cerr << "txn error: " << PQerrorMessage(p_->conn);
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
    return result;
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
    std::lock_guard<std::mutex> lock(p_->db_mutex);

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