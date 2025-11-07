#include "server.h"
#include <string>
#include <iostream>

static InlineCache::Policy parse_policy(int argc, char** argv) {
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        const std::string pfx = "--policy=";
        if (arg.rfind(pfx, 0) == 0) {
            std::string v = arg.substr(pfx.size());
            if (v == "lru") return InlineCache::Policy::LRU;
            if (v == "fifo") return InlineCache::Policy::FIFO;
            if (v == "random") return InlineCache::Policy::Random;
            std::cerr << "Unknown policy '" << v << "', defaulting to LRU\n";
        }
    }
    return InlineCache::Policy::LRU;
}

int main(int argc, char** argv) {
    InlineCache::Policy policy = parse_policy(argc, argv);
    KeyValueServer server{"localhost", 2222, policy};
    server.setupRoutes();
    server.start();
    return 0;
}
