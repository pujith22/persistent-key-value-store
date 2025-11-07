#include "persistence_adapter.h"

#include <iostream>
#include <sstream>
#include <libpq-fe.h>

// Implementation of PersistenceAdapter using libpq (PostgreSQL C client)

struct PersistenceAdapter::Impl {
    PGconn* conn{nullptr};
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

    const char* sql =
        "INSERT INTO kv_store (key, value) VALUES ($1::int, $2::text) "
        "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, created_at = now();";

    std::string keyStr = to_string_int(key);
    const char* params[2] = { keyStr.c_str(), value.c_str() };

    PGresult* res = PQexecParams(
        p_->conn,
        sql,
        2,           // number of params
        nullptr,     // let the server infer types
        params,
        nullptr,     // param lengths (all text)
        nullptr,     // param formats (all text)
        0            // result in text format
    );

    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        std::cerr << "insert() error: " << PQerrorMessage(p_->conn);
        PQclear(res);
        return false;
    }

    PQclear(res);
    return true;
}

bool PersistenceAdapter::remove(int key)
{
    if (!p_ || !p_->conn) return false;

    const char* sql = "DELETE FROM kv_store WHERE key = $1::int;";
    std::string keyStr = to_string_int(key);
    const char* params[1] = { keyStr.c_str() };

    PGresult* res = PQexecParams(
        p_->conn,
        sql,
        1,
        nullptr,
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

    const char* sql = "SELECT value FROM kv_store WHERE key = $1::int;";
    std::string keyStr = to_string_int(key);
    const char* params[1] = { keyStr.c_str() };

    PGresult* res = PQexecParams(
        p_->conn,
        sql,
        1,
        nullptr,
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

