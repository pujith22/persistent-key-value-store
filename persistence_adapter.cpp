#include "persistence_adapter.h"

#include <iostream>
#include <sstream>
#include <libpq-fe.h>

// Implementation of PersistenceAdapter using libpq (PostgreSQL C client)

struct PersistenceAdapter::Impl {
    PGconn* conn{nullptr};
    bool prepared{false};
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

