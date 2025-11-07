#pragma once

#include <string>
#include <vector>
#include <httplib.h>
#include <nlohmann/json.hpp>
#include <chrono>
#include "inline_cache.h"

// KeyValueServer: wraps httplib::Server providing route setup and lifecycle control.
// Responsibility:
//  - Register HTTP routes (home, get_key, bulk_query, insert, bulk_update, delete_key, update_key, stop)
//  - Start listening on a host:port
//  - Provide structured logging of requests and responses
//  - (Future) integrate PersistenceAdapter for DB operations
//
// Usage:
//  KeyValueServer srv{"localhost", 2222};
//  srv.setupRoutes();
//  srv.start();
//
class KeyValueServer {
public:
    KeyValueServer(const std::string& host, int port, InlineCache::Policy = InlineCache::Policy::LRU,  bool jsonLogging = false);
    ~KeyValueServer();

    // Register all routes on the underlying server instance.
    void setupRoutes();

    // Start blocking listen call. Returns when server stops or fails.
    bool start();

    // Signal server to stop (non-blocking). Safe to call from handler.
    void stop();

    // Access underlying httplib server if advanced customization needed.
    httplib::Server& raw();

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
    static void json_response(httplib::Response& res, int status, const nlohmann::json& j);
    static bool parse_int(const std::string& s, int& out);

    // logging mode flag
    bool json_logging_enabled{false};
    std::chrono::steady_clock::time_point server_boot_time{};
};
