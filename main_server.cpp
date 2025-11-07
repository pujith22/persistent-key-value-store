#include "server.h"
#include <string>
#include <iostream>

static InlineCache::Policy parse_policy(int argc, char** argv) {
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        const std::string pfx = "--policy=";
        if (arg.rfind(pfx, 0) == 0) {
            std::string v = arg.substr(pfx.size());
            if (v == "lru" || v == "LRU") return InlineCache::Policy::LRU;
            if (v == "fifo" || v == "FIFO") return InlineCache::Policy::FIFO;
            if (v == "random" || v == "RANDOM") return InlineCache::Policy::Random;
            std::cerr << "Unknown policy '" << v << "', defaulting to LRU\n";
        }
    }
    return InlineCache::Policy::LRU;
}

static bool parse_json_logging(int argc, char** argv) {
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--json-logs" || arg == "--log=json") return true;
    }
    return false;
}

int main(int argc, char** argv) {
    InlineCache::Policy policy = parse_policy(argc, argv);
    bool enable_json_logging = parse_json_logging(argc, argv);
    KeyValueServer server{"localhost", 2222, policy, enable_json_logging};
    server.setupRoutes();
    if (!server.start()) {
        return 1;
    }
    return 0;
}