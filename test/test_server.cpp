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

    // 2) GET /get_key/:key_id (initially not found)
    if (auto res = cli.Get("/get_key/123")) {
        fails += !expect(res->status == 200, "GET /get_key/123 should return 200");
        fails += !expect(res->body.find("Query for key: 123") != std::string::npos, "Body should mention queried key");
        fails += !expect(res->body.find("not found") != std::string::npos, "Initial GET should report not found");
    } else { std::cerr << "GET /get_key failed\n"; ++fails; }

    // 3) POST /bulk_query
    if (auto res = cli.Post("/bulk_query")) {
        fails += !expect(res->status == 200, "POST /bulk_query should return 200");
        fails += !expect(res->body.find("Bulk query") != std::string::npos, "Bulk query body hint");
    } else { std::cerr << "POST /bulk_query failed\n"; ++fails; }

    // 4) POST /insert/:key/:value (echo body JSON + populate cache)
    const char* insert_body = "{\"k\":1,\"v\":\"abc\"}";
    if (auto res = cli.Post("/insert/1/abc", insert_body, "application/json")) {
        fails += !expect(res->status == 200, "POST /insert should return 200");
        fails += !expect(res->get_header_value("Content-Type").find("text/json") != std::string::npos, "insert content-type should be text/json");
        fails += !expect(res->body == insert_body, "insert should echo JSON body");
    } else { std::cerr << "POST /insert failed\n"; ++fails; }
    // verify cache hit
    if (auto res = cli.Get("/get_key/1")) {
        fails += !expect(res->body.find("value: abc") != std::string::npos, "GET /get_key/1 should show cached value abc");
    } else { std::cerr << "GET /get_key/1 failed\n"; ++fails; }

    // 5) PATCH /bulk_update (echo body JSON)
    const char* patch_body = "{\"ops\":[1,2,3]}";
    if (auto res = cli.Patch("/bulk_update", patch_body, "application/json")) {
        fails += !expect(res->status == 200, "PATCH /bulk_update should return 200");
        fails += !expect(res->body == patch_body, "bulk_update should echo JSON body");
    } else { std::cerr << "PATCH /bulk_update failed\n"; ++fails; }

    // 6) DELETE /delete_key/:key remove from cache
    if (auto res = cli.Delete("/delete_key/1")) {
        fails += !expect(res->status == 200, "DELETE /delete_key/1 should return 200");
        fails += !expect(res->body.find("Deletion endpoint") != std::string::npos, "deletion body message");
    } else { std::cerr << "DELETE /delete_key failed\n"; ++fails; }
    if (auto res = cli.Get("/get_key/1")) {
        fails += !expect(res->body.find("not found") != std::string::npos, "GET /get_key/1 should report not found after deletion");
    } else { std::cerr << "GET /get_key/1 after delete failed\n"; ++fails; }

    // 7) PUT /update_key/:key/:value (update only if key present - reinserting first)
    if (auto res = cli.Post("/insert/1/abc", insert_body, "application/json")) {
        (void)res;
    }
    const char* put_body = "{\"v\":\"new\"}";
    if (auto res = cli.Put("/update_key/1/new", put_body, "application/json")) {
        fails += !expect(res->status == 200, "PUT /update_key should return 200");
        fails += !expect(res->body == put_body, "update should echo JSON body");
    } else { std::cerr << "PUT /update_key failed\n"; ++fails; }
    if (auto res = cli.Get("/get_key/1")) {
        fails += !expect(res->body.find("value: new") != std::string::npos, "GET /get_key/1 should show updated value new");
    } else { std::cerr << "GET /get_key/1 after update failed\n"; ++fails; }

    // 8) 404 route
    if (auto res = cli.Get("/no_such_route")) {
        fails += !expect(res->status == 404, "Unknown route should return 404");
    } else { std::cerr << "GET unknown route failed to connect\n"; ++fails; }

    // 9) Stop endpoint should stop the server, subsequent requests fail
    if (auto res = cli.Get("/stop")) {
        // allow server to stop
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
