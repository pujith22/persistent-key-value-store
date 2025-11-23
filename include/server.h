#pragma once

#include <string>
#include <vector>
#include <httplib.h>
#include <nlohmann/json.hpp>
#include <chrono>
#include "inline_cache.h"
#include "config.h"
#include "persistence_adapter.h"

// KeyValueServer: wraps httplib::Server providing route setup and lifecycle control.
// Responsibility:
//  - Register HTTP routes (home, get_key, bulk_query, insert, bulk_update, delete_key, update_key, stop)
//  - Start listening on a host:port
//  - Provide structured logging of requests and responses
//  - Require a PersistenceAdapter for DB operations (server startup fails without one)
//
// Usage:
//  KeyValueServer srv{"localhost", 2222};
//  srv.setupRoutes();
//  srv.start();
//
class KeyValueServer {
public:
    KeyValueServer(const std::string& host, int port, InlineCache::Policy = InlineCache::Policy::LRU, bool json_logging = false);
    ~KeyValueServer();

    // Register all routes on the underlying server instance.
    void setupRoutes();

    // Start blocking listen call. Returns when server stops or fails.
    bool start();

    // Signal server to stop (non-blocking). Safe to call from handler.
    void stop();

    // Access underlying httplib server if advanced customization needed.
    httplib::Server& raw();

    // Allow skipping preload of cache on startup
    void setSkipPreload(bool skip) { skip_preload = skip; }

    // Allow tests/consumers to inject custom persistence implementation.
    void setPersistenceProvider(std::unique_ptr<PersistenceProvider> provider, const std::string& status_label = "injected");

    PersistenceProvider* persistence() const { return persistence_adapter.get(); }

private:
    std::string host_;
    int port_{};
    httplib::Server server_;
    InlineCache inline_cache;

    struct RouteDescriptor {
        const char* method;
        const char* path;
        const char* description;
    };

    static const std::vector<RouteDescriptor> routes_json;
    static constexpr const char* home_page_template_path = "assets/home.html";

    std::string renderHomePage() const;

    // ---- Handlers ----
    void indexHandler(const httplib::Request& req, httplib::Response& res);
    void homeHandler(const httplib::Request& req, httplib::Response& res);
    void getKeyHandler(const httplib::Request& req, httplib::Response& res);
    void bulkQueryHandler(const httplib::Request& req, httplib::Response& res);
    void insertionHandler(const httplib::Request& req, httplib::Response& res);
    void bulkUpdateHandler(const httplib::Request& req, httplib::Response& res);
    void deletionHandler(const httplib::Request& req, httplib::Response& res);
    void updationHandler(const httplib::Request& req, httplib::Response& res);
    void healthHandler(const httplib::Request& req, httplib::Response& res);
    void metricsHandler(const httplib::Request& req, httplib::Response& res);
    void stopHandler(const httplib::Request& req, httplib::Response& res);

    // Logging helpers
    void logRequest(const httplib::Request& req);
    void logResponse(const httplib::Response& res, std::chrono::steady_clock::duration duration);

    // Helpers
    static void json_response(httplib::Response& res, int status, const nlohmann::json& j, const char* reason = nullptr);
    static bool parse_int(const std::string& s, int& out);

    // logging mode flag
    bool json_logging_enabled{false};
    std::chrono::steady_clock::time_point server_boot_time{};
    // persistence adapter is required; start() fails fast if unavailable
    std::unique_ptr<PersistenceProvider> persistence_adapter;
    bool persistence_injected{false};

    // if true, do not perform preload of keys from persistence_adapter during start()
    bool skip_preload{false};

    // cached DB connection status message
    std::string db_connection_status;
};
