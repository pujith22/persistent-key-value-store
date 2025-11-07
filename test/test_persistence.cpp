#include "persistence_adapter.h"
#include <iostream>

int main()
{
    // Attempt to use local kvstore DB created earlier. Adjust conninfo if needed.
    const std::string conninfo = "dbname=kvstore user=pujith22 host=127.0.0.1 port=5432 password=password";

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
