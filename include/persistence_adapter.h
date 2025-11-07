#pragma once

#include <string>
#include <memory>

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

    // remove a key. Returns true if a row was deleted.
    bool remove(int key);

    // retrieve a value for a key. Returns nullptr if not found or on error.
    std::unique_ptr<std::string> get(int key);

private:
    struct Impl;               // PImpl to avoid exposing libpq headers in the public header
    std::unique_ptr<Impl> p_;  // owns the PG connection
};