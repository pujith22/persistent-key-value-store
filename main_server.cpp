#include "server.h"
#include <string>
#include <iostream>
#include <fstream>
#include <cstdlib>

// Simple .env loader: reads lines of the form KEY=VALUE and sets environment variables
static void load_dotenv(const std::string& path = ".env") {
    std::ifstream f(path);
    if (!f.is_open()) return;
    std::string line;
    while (std::getline(f, line)) {
        // trim whitespace
        if (line.empty()) continue;
        if (line[0] == '#') continue;
        auto eq = line.find('=');
        if (eq == std::string::npos) continue;
        std::string key = line.substr(0, eq);
        std::string val = line.substr(eq + 1);
        // trim spaces around key and val
        auto trim = [](std::string &s) {
            size_t a = s.find_first_not_of(" \t\r\n");
            size_t b = s.find_last_not_of(" \t\r\n");
            if (a == std::string::npos) { s.clear(); return; }
            s = s.substr(a, b - a + 1);
        };
        trim(key);
        trim(val);
        if (!key.empty()) {
            // set environment variable (overwrite existing)
            #ifdef _WIN32
            _putenv_s(key.c_str(), val.c_str());
            #else
            setenv(key.c_str(), val.c_str(), 1);
            #endif
        }
    }
}

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

static bool parse_no_logging(int argc, char** argv) {
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--no-logging" || arg == "--no-logs") return true;
    }
    return false;
}

static bool parse_no_metrics(int argc, char** argv) {
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--no-metrics" || arg == "--disable-metrics") return true;
    }
    return false;
}

static bool parse_skip_preload(int argc, char** argv) {
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--no-preload" || arg == "--skip-preload") return true;
    }
    return false;
}

int main(int argc, char** argv) {
    InlineCache::Policy policy = parse_policy(argc, argv);
    bool enable_json_logging = parse_json_logging(argc, argv);
    // load .env (if present) so SERVER_HOST and SERVER_PORT can be provided there
    load_dotenv();

    const char* host_env = std::getenv("SERVER_HOST");
    const char* port_env = std::getenv("SERVER_PORT");
    std::string host = host_env ? host_env : "0.0.0.0";
    int port = 2222;
    if (port_env) {
        try {
            port = std::stoi(port_env);
        } catch (...) {
            std::cerr << "Invalid SERVER_PORT value '" << port_env << "', using 2222\n";
        }
    }

    KeyValueServer server{host, port, policy, enable_json_logging};
    bool disable_logging = parse_no_logging(argc, argv);
    if (disable_logging) server.setLoggingEnabled(false);
    bool disable_metrics = parse_no_metrics(argc, argv);
    if (disable_metrics) server.setMetricsEnabled(false);
    bool skip_preload = parse_skip_preload(argc, argv);
    if (skip_preload) server.setSkipPreload(true);
    server.setupRoutes();
    if (!server.start()) {
        return 1;
    }
    return 0;
}