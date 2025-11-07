#include "server.h"
#include <httplib.h>
#include <nlohmann/json.hpp>
#include <iostream>
#include <thread>
#include <chrono>

using namespace std::chrono_literals;

static bool expect(bool cond, const char* msg) {
    if (!cond) std::cerr << "ASSERT FAILED: " << msg << "\n";
    return cond;
}

static bool wait_until_up(const std::string& host, int port, int retries = 100, int ms = 20) {
    httplib::Client cli(host, port);
    cli.set_connection_timeout(0, 200000); // 200ms
    for (int i = 0; i < retries; ++i) {
        if (auto res = cli.Get("/")) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(ms));
    }
    return false;
}

int main() {
    const std::string host = "localhost";
    const int port = 23876; // test port

    KeyValueServer server{host, port};
    server.setupRoutes();

    // start server in background thread
    std::thread t([&](){ server.start(); });

    if (!wait_until_up(host, port)) {
        std::cerr << "Server did not start listening in time" << std::endl;
        if (t.joinable()) server.stop(), t.join();
        return 2;
    }

    int fails = 0;
    httplib::Client cli(host, port);
    cli.set_connection_timeout(0, 500000);
    cli.set_read_timeout(1, 0);

    // 1) GET / and /home
    if (auto res = cli.Get("/")) {
        fails += !expect(res->status == 200, "GET / should return 200");
        fails += !expect(res->get_header_value("Content-Type").find("application/json") != std::string::npos, "GET / should be JSON");
        auto body = nlohmann::json::parse(res->body);
        fails += !expect(body.contains("routes"), "Service catalog should list routes");
        fails += !expect(body["routes"].is_array(), "Service catalog routes should be array");
        bool hasHomeRoute = false;
        for (const auto& route : body["routes"]) {
            if (route.value("path", std::string()) == "/home") {
                hasHomeRoute = true;
                break;
            }
        }
        fails += !expect(hasHomeRoute, "Service catalog should include /home route");
    } else { std::cerr << "GET / failed to connect\n"; ++fails; }

    if (auto res = cli.Get("/home")) {
        fails += !expect(res->status == 200, "GET /home should return 200");
        fails += !expect(res->get_header_value("Content-Type").find("text/html") != std::string::npos, "GET /home should be HTML");
        fails += !expect(res->body.find("Available Routes") != std::string::npos, "/home should render route table");
    } else { std::cerr << "GET /home failed\n"; ++fails; }

    // 2) GET /get_key/:key_id (initially not found => 404 JSON)
    if (auto res = cli.Get("/get_key/123")) {
        fails += !expect(res->status == 404, "GET /get_key/123 should return 404 for missing key");
        fails += !expect(res->get_header_value("Content-Type").find("application/json") != std::string::npos, "get_key should be JSON");
        auto body = nlohmann::json::parse(res->body);
        fails += !expect(body.value("found", true) == false, "JSON should indicate found:false");
    fails += !expect(body.value("reason", "").find("not present") != std::string::npos, "Missing key response should include reason");
    } else { std::cerr << "GET /get_key failed\n"; ++fails; }
    // invalid key format -> 400
    if (auto res = cli.Get("/get_key/not-a-number")) {
        fails += !expect(res->status == 400, "GET invalid key should return 400");
        auto body = nlohmann::json::parse(res->body);
        fails += !expect(body.value("error", "") == "invalid key format", "Invalid key error message");
        fails += !expect(body.contains("reason"), "Invalid key response should include reason field");
    } else { std::cerr << "GET /get_key invalid failed\n"; ++fails; }

    // 3) POST /bulk_query (empty body -> JSON info)
    if (auto res = cli.Post("/bulk_query")) {
        fails += !expect(res->status == 200, "POST /bulk_query should return 200");
        fails += !expect(res->get_header_value("Content-Type").find("application/json") != std::string::npos, "bulk_query should be JSON");
        fails += !expect(res->body.find("\"endpoint\":\"bulk_query\"") != std::string::npos, "Bulk query JSON should include endpoint");
    } else { std::cerr << "POST /bulk_query failed\n"; ++fails; }

    // 4) POST /insert/:key/:value -> 201 JSON
    if (auto res = cli.Post("/insert/1/abc", "", "application/json")) {
        fails += !expect(res->status == 201, "POST /insert should return 201 on create");
        auto body = nlohmann::json::parse(res->body);
        fails += !expect(body.value("created", false) == true, "Insert JSON should mark created");
    } else { std::cerr << "POST /insert failed\n"; ++fails; }
    if (auto res = cli.Post("/insert/notnum/abc", "", "application/json")) {
        fails += !expect(res->status == 400, "POST invalid key should return 400");
        {
            auto body = nlohmann::json::parse(res->body);
            fails += !expect(body.contains("reason"), "Insert invalid key should include reason in response");
        }
    } else { std::cerr << "POST /insert invalid failed\n"; ++fails; }
    // verify cache hit now returns 200 with found:true
    if (auto res = cli.Get("/get_key/1")) {
        fails += !expect(res->status == 200, "GET /get_key/1 should return 200 for existing key");
        auto body = nlohmann::json::parse(res->body);
        fails += !expect(body.value("found", false) == true, "GET /get_key/1 JSON found:true");
        fails += !expect(body.value("value", "") == "abc", "GET /get_key/1 should include value 'abc'");
    } else { std::cerr << "GET /get_key/1 failed\n"; ++fails; }
    // conflict insert -> 409
    if (auto res = cli.Post("/insert/1/duplicate", "", "application/json")) {
        fails += !expect(res->status == 409, "POST /insert existing should return 409");
        auto body = nlohmann::json::parse(res->body);
        fails += !expect(body.contains("existing_value"), "Conflict response should include existing_value");
    fails += !expect(body.value("reason", "").find("exists") != std::string::npos, "Conflict response should include reason");
    } else { std::cerr << "POST /insert conflict failed\n"; ++fails; }

    // 5) PATCH /bulk_update (echo body JSON)
    // 5) PATCH /bulk_update with ops array
    const char* patch_array = "[{\"op\":\"update_or_insert\",\"key\":1,\"value\":\"abc\"}]";
    if (auto res = cli.Patch("/bulk_update", patch_array, "application/json")) {
        fails += !expect(res->status == 200, "PATCH /bulk_update should return 200");
        auto body = nlohmann::json::parse(res->body);
        fails += !expect(body.contains("results"), "bulk_update should produce results array");
    } else { std::cerr << "PATCH /bulk_update failed\n"; ++fails; }
    // invalid bulk_update payload -> 400
    if (auto res = cli.Patch("/bulk_update", "{\"bad\":1}", "application/json")) {
        fails += !expect(res->status == 400, "PATCH /bulk_update invalid payload should return 400");
        auto body = nlohmann::json::parse(res->body);
    fails += !expect(body.value("reason", "").find("array") != std::string::npos, "Bulk update invalid payload should include reason");
    } else { std::cerr << "PATCH /bulk_update failed\n"; ++fails; }

    // invalid bulk query payload -> 400
    if (auto res = cli.Post("/bulk_query", "{\"unexpected\":true}", "application/json")) {
        fails += !expect(res->status == 400, "POST /bulk_query invalid payload should return 400");
        auto body = nlohmann::json::parse(res->body);
    fails += !expect(body.value("reason", "").find("array") != std::string::npos, "Bulk query invalid payload should indicate array expectation");
    } else { std::cerr << "POST /bulk_query invalid failed\n"; ++fails; }

    if (auto res = cli.Post("/bulk_query", "{bad json", "application/json")) {
        fails += !expect(res->status == 400, "POST /bulk_query malformed JSON should return 400");
        auto body = nlohmann::json::parse(res->body);
    fails += !expect(body.value("reason", "").find("parse") != std::string::npos, "Bulk query malformed JSON should include parse reason");
    } else { std::cerr << "POST /bulk_query malformed failed\n"; ++fails; }

    // 6) DELETE /delete_key/:key remove from cache -> 204
    if (auto res = cli.Delete("/delete_key/1")) {
        fails += !expect(res->status == 204, "DELETE /delete_key/1 should return 204 on success");
    } else { std::cerr << "DELETE /delete_key failed\n"; ++fails; }
    if (auto res = cli.Get("/get_key/1")) {
        fails += !expect(res->status == 404, "GET /get_key/1 should return 404 after deletion");
    } else { std::cerr << "GET /get_key/1 after delete failed\n"; ++fails; }
    // delete missing -> 404
    if (auto res = cli.Delete("/delete_key/9999")) {
        fails += !expect(res->status == 404, "DELETE missing key should return 404");
        auto body = nlohmann::json::parse(res->body);
    fails += !expect(body.value("reason", "").find("not present") != std::string::npos, "Delete missing key should include reason");
    } else { std::cerr << "DELETE missing failed\n"; ++fails; }

    // 7) PUT /update_key/:key/:value (reinserting first) -> 200
    if (auto res = cli.Post("/insert/1/abc", "", "application/json")) { /* created */ }
    if (auto res = cli.Put("/update_key/1/new", "", "application/json")) {
        fails += !expect(res->status == 200, "PUT /update_key should return 200 on success");
        auto body = nlohmann::json::parse(res->body);
        fails += !expect(body.value("updated", false) == true, "Update JSON should mark updated");
    } else { std::cerr << "PUT /update_key failed\n"; ++fails; }
    if (auto res = cli.Get("/get_key/1")) {
        auto body = nlohmann::json::parse(res->body);
        fails += !expect(body.value("value", "") == "new", "GET /get_key/1 should show updated value new");
    } else { std::cerr << "GET /get_key/1 after update failed\n"; ++fails; }
    // update missing -> 404
    if (auto res = cli.Put("/update_key/4242/x", "", "application/json")) {
        fails += !expect(res->status == 404, "PUT missing key should return 404");
        auto body = nlohmann::json::parse(res->body);
    fails += !expect(body.value("reason", "").find("not present") != std::string::npos, "Update missing key should include reason");
    } else { std::cerr << "PUT missing key failed\n"; ++fails; }
    if (auto res = cli.Put("/update_key/notnum/x", "", "application/json")) {
        fails += !expect(res->status == 400, "PUT invalid key should return 400");
    auto body = nlohmann::json::parse(res->body);
    fails += !expect(body.contains("reason"), "Update invalid key should include reason");
    } else { std::cerr << "PUT invalid key failed\n"; ++fails; }

    if (auto res = cli.Delete("/delete_key/notnum")) {
        fails += !expect(res->status == 400, "DELETE invalid key should return 400");
        {
            auto body = nlohmann::json::parse(res->body);
            fails += !expect(body.contains("reason"), "DELETE invalid key should include reason in response");
        }
    } else { std::cerr << "DELETE invalid key failed\n"; ++fails; }

    // 8) 404 route (unknown)
    if (auto res = cli.Get("/no_such_route")) {
        fails += !expect(res->status == 404, "Unknown route should return 404");
    } else { std::cerr << "GET unknown route failed to connect\n"; ++fails; }

    // 9) Health & metrics endpoints
    if (auto res = cli.Get("/health")) {
        fails += !expect(res->status == 200, "/health should return 200");
        auto body = nlohmann::json::parse(res->body);
        fails += !expect(body.value("status", "") == "ok", "/health status should be ok");
        fails += !expect(body.value("uptime_ms", 0) >= 0, "/health uptime should be non-negative");
    } else { std::cerr << "GET /health failed\n"; ++fails; }

    if (auto res = cli.Get("/metrics")) {
        fails += !expect(res->status == 200, "/metrics should return 200");
        auto body = nlohmann::json::parse(res->body);
        fails += !expect(body.contains("entries"), "/metrics should include entries");
        fails += !expect(body.contains("hits"), "/metrics should include hits");
        fails += !expect(body.contains("misses"), "/metrics should include misses");
    } else { std::cerr << "GET /metrics failed\n"; ++fails; }

    // 10) Stop endpoint should stop the server, subsequent requests fail
    if (auto res = cli.Get("/stop")) {
        fails += !expect(res->status == 200, "/stop should return 200");
        auto body = nlohmann::json::parse(res->body);
        fails += !expect(body.value("stopping", false) == true, "/stop body should indicate stopping");
        std::this_thread::sleep_for(100ms);
    }

    // After stop, expect connection failure
    {
        httplib::Client cli2(host, port);
        cli2.set_connection_timeout(0, 200000);
        auto res2 = cli2.Get("/");
        fails += !expect(!res2, "After /stop, server should not accept connections");
    }

    if (t.joinable()) t.join();

    if (fails == 0) {
        std::cout << "All server tests passed." << std::endl;
        return 0;
    }
    std::cerr << fails << " server test(s) failed." << std::endl;
    return 1;
}
