#include "server.h"
#include <httplib.h>
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
        fails += !expect(res->get_header_value("Content-Type").find("text/html") != std::string::npos, "GET / should be text/html");
        fails += !expect(res->body.find("Welcome to Persistent Key Value Server!!") != std::string::npos, "Home page should contain welcome text");
    } else { std::cerr << "GET / failed to connect\n"; ++fails; }

    if (auto res = cli.Get("/home")) {
        fails += !expect(res->status == 200, "GET /home should return 200");
    } else { std::cerr << "GET /home failed\n"; ++fails; }

    // 2) GET /get_key/:key_id (initially not found => 404 JSON)
    if (auto res = cli.Get("/get_key/123")) {
        fails += !expect(res->status == 404, "GET /get_key/123 should return 404 for missing key");
        fails += !expect(res->get_header_value("Content-Type").find("application/json") != std::string::npos, "get_key should be JSON");
        fails += !expect(res->body.find("\"found\":false") != std::string::npos, "JSON should indicate found:false");
    } else { std::cerr << "GET /get_key failed\n"; ++fails; }

    // 3) POST /bulk_query (empty body -> JSON info)
    if (auto res = cli.Post("/bulk_query")) {
        fails += !expect(res->status == 200, "POST /bulk_query should return 200");
        fails += !expect(res->get_header_value("Content-Type").find("application/json") != std::string::npos, "bulk_query should be JSON");
        fails += !expect(res->body.find("\"endpoint\":\"bulk_query\"") != std::string::npos, "Bulk query JSON should include endpoint");
    } else { std::cerr << "POST /bulk_query failed\n"; ++fails; }

    // 4) POST /insert/:key/:value -> 201 JSON
    if (auto res = cli.Post("/insert/1/abc", "", "application/json")) {
        fails += !expect(res->status == 201, "POST /insert should return 201 on create");
        fails += !expect(res->body.find("\"created\":true") != std::string::npos, "Insert JSON should mark created");
    } else { std::cerr << "POST /insert failed\n"; ++fails; }
    // verify cache hit now returns 200 with found:true
    if (auto res = cli.Get("/get_key/1")) {
        fails += !expect(res->status == 200, "GET /get_key/1 should return 200 for existing key");
        fails += !expect(res->body.find("\"found\":true") != std::string::npos, "GET /get_key/1 JSON found:true");
        fails += !expect(res->body.find("\"value\":\"abc\"") != std::string::npos, "GET /get_key/1 should include value 'abc'");
    } else { std::cerr << "GET /get_key/1 failed\n"; ++fails; }

    // 5) PATCH /bulk_update (echo body JSON)
    // 5) PATCH /bulk_update with ops array
    const char* patch_array = "[{\"op\":\"upsert\",\"key\":1,\"value\":\"abc\"}]";
    if (auto res = cli.Patch("/bulk_update", patch_array, "application/json")) {
        fails += !expect(res->status == 200, "PATCH /bulk_update should return 200");
        fails += !expect(res->body.find("\"results\"") != std::string::npos, "bulk_update should produce results array");
    } else { std::cerr << "PATCH /bulk_update failed\n"; ++fails; }

    // 6) DELETE /delete_key/:key remove from cache -> 204
    if (auto res = cli.Delete("/delete_key/1")) {
        fails += !expect(res->status == 204, "DELETE /delete_key/1 should return 204 on success");
    } else { std::cerr << "DELETE /delete_key failed\n"; ++fails; }
    if (auto res = cli.Get("/get_key/1")) {
        fails += !expect(res->status == 404, "GET /get_key/1 should return 404 after deletion");
    } else { std::cerr << "GET /get_key/1 after delete failed\n"; ++fails; }

    // 7) PUT /update_key/:key/:value (reinserting first) -> 200
    if (auto res = cli.Post("/insert/1/abc", "", "application/json")) { /* created */ }
    if (auto res = cli.Put("/update_key/1/new", "", "application/json")) {
        fails += !expect(res->status == 200, "PUT /update_key should return 200 on success");
        fails += !expect(res->body.find("\"updated\":true") != std::string::npos, "Update JSON should mark updated");
    } else { std::cerr << "PUT /update_key failed\n"; ++fails; }
    if (auto res = cli.Get("/get_key/1")) {
        fails += !expect(res->body.find("\"value\":\"new\"") != std::string::npos, "GET /get_key/1 should show updated value new");
    } else { std::cerr << "GET /get_key/1 after update failed\n"; ++fails; }

    // 8) 404 route (unknown)
    if (auto res = cli.Get("/no_such_route")) {
        fails += !expect(res->status == 404, "Unknown route should return 404");
    } else { std::cerr << "GET unknown route failed to connect\n"; ++fails; }

    // 9) Stop endpoint should stop the server, subsequent requests fail
    if (auto res = cli.Get("/stop")) { std::this_thread::sleep_for(100ms); }

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
