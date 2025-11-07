#pragma once

#include <string>
#include <memory>
#include <vector>
#include "nlohmann/json.hpp"

// PersistenceAdapter: lightweight wrapper around PostgreSQL C client (libpq)
// to perform simple integer-keyed string-value operations.
//
// Usage: create with a libpq connection string (e.g. "dbname=kvstore user=...")

class PersistenceAdapter {
public:
    explicit PersistenceAdapter(const std::string &conninfo);
    ~PersistenceAdapter();

    // insert or update a key/value pair. Returns true on success.
    bool insert(int key, const std::string &value);

    // update an existing key's value. Returns true if a row was updated (key existed).
    bool update(int key, const std::string &value);

    // remove a key. Returns true if a row was deleted.
    bool remove(int key);

    // retrieve a value for a key. Returns nullptr if not found or on error.
    std::unique_ptr<std::string> get(int key);

    // Batch transactional execution with two modes
    enum class TxMode { RollbackOnError, Silent };
    enum class OpType { Insert, Update, Remove, Get };

    struct Operation {
        OpType type;
        int key;
        std::string value; // can be ignored for remove operation
    };

    struct FailedOp {
        Operation op;
        std::string error; // for storing error message
    };

    struct TxResult {
        bool success; // true if committed with no failures (RollbackOnError) or committed (Silent)
        std::vector<FailedOp> failures; // populated in Silent mode; in RollbackOnError contains at least the first failure
    };

    // Executes a sequence of operations in a transaction.
    // - RollbackOnError: abort whole txn on first failure (SQL error or 0 rows for update/remove)
    // - Silent: continue on failures using savepoints and commit successes and collect failures
    TxResult runTransaction(const std::vector<Operation>& ops, TxMode mode);

    // Same as runTransaction but returns a JSON report.
    // Example shape:
    // {
    //   "mode": "silent"|"rollback",
    //   "success": true|false,
    //   "results": [
    //      {"op":"insert","key":1,"status":"ok"},
    //      {"op":"get","key":2,"status":"ok","value":null},
    //      {"op":"update","key":3,"status":"failed","error":"no rows affected"}
    //   ]
    // }
    nlohmann::json runTransactionJson(const std::vector<Operation>& ops, TxMode mode);

private:
    struct Impl;               // PImpl to avoid exposing libpq headers in the public header
    std::unique_ptr<Impl> p_;  // owns the PG connection
};