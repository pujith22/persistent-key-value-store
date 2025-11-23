#pragma once

#include <string>
#include <memory>
#include <vector>
#include <future>
#include "nlohmann/json.hpp"

// PersistenceAdapter: lightweight wrapper around PostgreSQL C client (libpq)
// to perform simple integer-keyed string-value operations.
//
// Usage: create with a libpq connection string (e.g. "dbname=kvstore user=...")

class PersistenceProvider {
public:
    virtual ~PersistenceProvider() = default;

    virtual bool insert(int key, const std::string &value) = 0;
    virtual bool update(int key, const std::string &value) = 0;
    virtual bool remove(int key) = 0;
    virtual std::unique_ptr<std::string> get(int key) = 0;
};

class PersistenceAdapter : public PersistenceProvider {
public:
    explicit PersistenceAdapter(const std::string &conninfo);
    ~PersistenceAdapter();

    // insert or update a key/value pair. Returns true on success.
    bool insert(int key, const std::string &value) override;

    // update an existing key's value. Returns true if a row was updated (key existed).
    bool update(int key, const std::string &value) override;

    // remove a key. Returns true if a row was deleted.
    bool remove(int key) override;

    // retrieve a value for a key. Returns nullptr if not found or on error.
    std::unique_ptr<std::string> get(int key) override;

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

    /* Executes a sequence of operations in a transaction.
         - RollbackOnError: abort whole txn on first failure (SQL error or 0 rows for update/remove)
         - Silent: continue on failures using savepoints and commit successes and collect failures */
    TxResult runTransaction(const std::vector<Operation>& ops, TxMode mode);

    /* Same as runTransaction but returns a JSON report.
        Example shape:
        {
          "mode": "silent"|"rollback",
          "success": true|false,
          "results": [
             {"op":"insert","key":1,"status":"ok"},
             {"op":"get","key":2,"status":"ok","value":null},
             {"op":"update","key":3,"status":"failed","error":"no rows affected"}
           ]
    */
    nlohmann::json runTransactionJson(const std::vector<Operation>& ops, TxMode mode);

    // Async variants: submit work to an internal worker pool and return a future.
    // These are concrete APIs on the adapter (not part of the abstract PersistenceProvider).
    std::future<std::unique_ptr<std::string>> getAsync(int key);
    std::future<nlohmann::json> runTransactionJsonAsync(const std::vector<Operation>& ops, TxMode mode);

    // runtime metrics/accessors
    int droppedPoolConnections() const;
    // Return a JSON object with pool metrics: pool_size, free_conns, dropped_conns, total_conn_creates, total_conn_failures
    nlohmann::json poolMetrics() const;

private:
    struct Impl;               // PImpl to avoid exposing libpq headers in the public header
    std::unique_ptr<Impl> p_;  // owns the PG connection
};