#include "persistence_adapter.h"
#include "config.h"
#include <iostream>

int main()
{
    // Connection string from env PG_CONNINFO or config/db.json, fallback to dbname=kvstore
    const std::string conninfo = load_conninfo();

    try {
        PersistenceAdapter db{conninfo};

        std::cout << "Inserting key=10 -> 'hello'...\n";
        if (!db.insert(10, "hello")) {
            std::cerr << "Insert failed\n";
            return 1;
        }

        std::cout << "Getting key=10...\n";
        auto v = db.get(10);
        if (v) std::cout << "Retrieved: " << *v << '\n';
        else std::cout << "Key 10 not found\n";

        // Update existing key
        std::cout << "Updating key=10 -> 'world'...\n";
        bool upd_existing = db.update(10, "world");
        std::cout << (upd_existing ? "Updated key 10\n" : "Update failed for key 10\n");
        auto v2 = db.get(10);
        if (!v2 || *v2 != std::string("world")) {
            std::cerr << "Update verification failed for key 10\n";
            return 1;
        }

        // Update non-existent key
        std::cout << "Updating non-existent key=999 -> 'noop'...\n";
        bool upd_missing = db.update(999, "noop");
        std::cout << (upd_missing ? "Unexpectedly updated missing key\n" : "Correctly did not update missing key\n");

        std::cout << "Deleting key=10...\n";
        if (db.remove(10)) std::cout << "Deleted key 10\n";
        else std::cout << "Key 10 not deleted (missing?)\n";

        std::cout << "Verifying deletion...\n";
        v = db.get(10);
        if (!v) std::cout << "Key 10 absent as expected\n";
        else std::cout << "Key 10 still present: " << *v << '\n';

        return 0;
    } catch (const std::exception &e) {
        std::cerr << "Fatal: " << e.what() << '\n';
        return 2;
    }
}
