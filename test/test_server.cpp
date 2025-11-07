#include "server.h"
#include <httplib.h>
#include <nlohmann/json.hpp>
#include <iostream>
#include <thread>
#include <chrono>
#include <unordered_map>
#include <mutex>
#include <optional>

using namespace std::chrono_literals;

static bool expect(bool cond, const char* msg) {
    if (!cond) std::cerr << "ASSERT FAILED: " << msg << "\n";
    return cond;
}

struct FakePersistence : PersistenceProvider {
    bool insert(int key, const std::string& value) override {
        std::lock_guard<std::mutex> lock(mtx);
        ++insert_calls;
        store[key] = value;
        return true;
    }

    bool update(int key, const std::string& value) override {
        std::lock_guard<std::mutex> lock(mtx);
        ++update_calls;
        auto it = store.find(key);
        if (it == store.end()) return false;
        it->second = value;
        return true;
    }

    bool remove(int key) override {
        std::lock_guard<std::mutex> lock(mtx);
        ++remove_calls;
        return store.erase(key) > 0;
    }

    std::unique_ptr<std::string> get(int key) override {
        std::lock_guard<std::mutex> lock(mtx);
        ++get_calls;
        auto it = store.find(key);
        if (it == store.end()) return nullptr;
        return std::make_unique<std::string>(it->second);
    }

    void setDirect(int key, const std::string& value) {
        std::lock_guard<std::mutex> lock(mtx);
        store[key] = value;
    }

    void eraseDirect(int key) {
        std::lock_guard<std::mutex> lock(mtx);
        store.erase(key);
    }

    std::optional<std::string> valueFor(int key) const {
        std::lock_guard<std::mutex> lock(mtx);
        auto it = store.find(key);
        if (it == store.end()) return std::nullopt;
        return it->second;
    }

    int getCallCount() const {
        std::lock_guard<std::mutex> lock(mtx);
        return get_calls;
    }

    int insertCallCount() const {
        std::lock_guard<std::mutex> lock(mtx);
        return insert_calls;
    }

    int updateCallCount() const {
        std::lock_guard<std::mutex> lock(mtx);
        return update_calls;
    }

    int removeCallCount() const {
        std::lock_guard<std::mutex> lock(mtx);
        return remove_calls;
    }

private:
    mutable std::mutex mtx;
    std::unordered_map<int, std::string> store;
    mutable int insert_calls{0};
    mutable int update_calls{0};
    mutable int remove_calls{0};
    mutable int get_calls{0};
};

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
    auto fakePersistence = std::make_unique<FakePersistence>();
    auto* fake = fakePersistence.get();
    fake->setDirect(222, "db-only");
    fake->setDirect(333, "bulk-db");
    server.setPersistenceProvider(std::move(fakePersistence), "test-double");
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

    // Read-through from persistence: key 222 only exists in fake persistence at start
    if (auto res = cli.Get("/get_key/222")) {
        fails += !expect(res->status == 200, "GET /get_key/222 should return 200 via persistence");
        auto body = nlohmann::json::parse(res->body);
        fails += !expect(body.value("value", "") == "db-only", "Read-through should return persistence value");
        fails += !expect(body.value("source", "") == "persistence", "Read-through should note persistence source");
    } else { std::cerr << "GET /get_key/222 failed\n"; ++fails; }

    // Cache should now serve without hitting persistence again even if DB value changes
    fake->setDirect(222, "db-updated");
    if (auto res = cli.Get("/get_key/222")) {
        auto body = nlohmann::json::parse(res->body);
        fails += !expect(body.value("value", "") == "db-only", "Cached value should be served on subsequent GET");
    } else { std::cerr << "GET /get_key/222 second attempt failed\n"; ++fails; }
    fails += !expect(fake->getCallCount() >= 1, "Persistence get should be called at least once for read-through");

    // 3) PATCH /bulk_query (empty body -> JSON errors but HTTP 200)
    if (auto res = cli.Patch("/bulk_query")) {
        fails += !expect(res->status == 200, "PATCH /bulk_query should return 200 even for empty body");
        fails += !expect(res->get_header_value("Content-Type").find("application/json") != std::string::npos, "bulk_query should be JSON");
        auto body = nlohmann::json::parse(res->body);
        fails += !expect(body.contains("endpoint") && body["endpoint"]=="bulk_query", "Bulk query JSON should include endpoint");
        fails += !expect(body.contains("results") && body["results"].is_array() && body["results"].empty(), "Empty body bulk query should return empty results array");
        bool sawEmptyError = false;
        if (body.contains("errors") && body["errors"].is_array()) {
            for (const auto& err : body["errors"]) {
                if (err.value("code", std::string()) == "empty_body") {
                    sawEmptyError = true;
                    break;
                }
            }
        }
        fails += !expect(sawEmptyError, "Empty body bulk query should report empty_body error");
    } else { std::cerr << "PATCH /bulk_query failed\n"; ++fails; }

    // 4) POST /insert/:key/:value -> 201 JSON
    if (auto res = cli.Post("/insert/1/abc", "", "application/json")) {
        fails += !expect(res->status == 201, "POST /insert should return 201 on create");
        auto body = nlohmann::json::parse(res->body);
        fails += !expect(body.value("created", false) == true, "Insert JSON should mark created");
        fails += !expect(fake->valueFor(1).value_or("") == "abc", "Insert should persist value");
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

    // 5) POST /bulk_update accepts transactional operations
    const char* patch_payload = R"({"operations":[{"operation":"insert","key":777,"value":"txn-ins"},{"operation":"get","key":777},{"operation":"update","key":777,"value":"txn-upd"},{"operation":"delete","key":777}]})";
    if (auto res = cli.Post("/bulk_update", patch_payload, "application/json")) {
        fails += !expect(res->status == 200, "POST /bulk_update should return 200");
        auto body = nlohmann::json::parse(res->body);
        fails += !expect(body.value("success", false) == true, "Successful bulk_update should report success true");
    auto mode = body.value("transaction_mode", std::string());
    bool tx_mode_ok = (mode == "rollback" || mode == "emulated");
    fails += !expect(tx_mode_ok, "Successful bulk_update should report transactional mode");
        fails += !expect(body.contains("summary") && body["summary"].value("succeeded", 0) == 4, "Summary should report four successful operations");
        fails += !expect(body.contains("results") && body["results"].is_array() && body["results"].size() == 4, "Results array should list each operation");
        auto& txn_results = body["results"];
        fails += !expect(txn_results[1].value("operation", "") == "get", "Second result should represent get operation");
        fails += !expect(txn_results[1].contains("value") && txn_results[1]["value"] == "txn-ins", "Get operation should include retrieved value");
        fails += !expect(txn_results[3].value("operation", "") == "delete", "Fourth result should represent delete operation");
    } else { std::cerr << "POST /bulk_update failed\n"; ++fails; }
    if (auto res = cli.Get("/get_key/777")) {
        fails += !expect(res->status == 404, "Key 777 should be deleted after transactional bulk_update");
    } else { std::cerr << "GET /get_key/777 failed\n"; ++fails; }

    // Transaction should roll back on failure
    const char* patch_failure = R"({"operations":[{"operation":"insert","key":888,"value":"should-rollback"},{"operation":"update","key":9999,"value":"fails"},{"operation":"delete","key":888}]})";
    if (auto res = cli.Post("/bulk_update", patch_failure, "application/json")) {
        fails += !expect(res->status == 200, "POST /bulk_update failure should still return 200");
        auto body = nlohmann::json::parse(res->body);
        fails += !expect(body.value("success", true) == false, "Failed bulk_update should report success false");
        fails += !expect(body.contains("results") && body["results"].is_array() && body["results"].size() == 2, "Rollback response should include processed operations only");
        fails += !expect(body.contains("summary") && body["summary"].value("aborted", 0) == 1, "Summary should report one aborted operation");
        bool sawFailure = false;
        for (const auto& item : body["results"]) {
            if (item.value("status", "") == "failed") {
                sawFailure = true;
                fails += !expect(item.contains("error"), "Failed result should include error");
            }
        }
        fails += !expect(sawFailure, "Rollback response should include failing operation details");
    } else { std::cerr << "POST /bulk_update rollback failed\n"; ++fails; }
    if (auto res = cli.Get("/get_key/888")) {
        fails += !expect(res->status == 404, "Failed transaction should roll back inserted key 888");
    } else { std::cerr << "GET /get_key/888 failed\n"; ++fails; }
    // Bulk query should accept object payload with data array and provide verbose results
    const char* bulk_payload = "{\"data\":[222,333,\"oops\",444]}";
    if (auto res = cli.Patch("/bulk_query", bulk_payload, "application/json")) {
        fails += !expect(res->status == 200, "PATCH /bulk_query should return 200 for valid payload");
        auto body = nlohmann::json::parse(res->body);
        fails += !expect(body.contains("results") && body["results"].is_array(), "Bulk query should include results array");
        fails += !expect(body["results"].size() == 4, "Bulk query should report one entry per requested key");
    bool noTopErrors = !body.contains("errors") || (body["errors"].is_array() && body["errors"].empty());
    fails += !expect(noTopErrors, "Valid bulk query should have no top-level errors");

        auto find_result = [&](auto predicate) -> nlohmann::json {
            for (const auto& item : body["results"]) {
                if (item.is_object() && predicate(item)) return item;
            }
            return nlohmann::json();
        };

        auto r222 = find_result([&](const nlohmann::json& item){ return item.value("key", 0) == 222; });
        fails += !expect(!r222.is_null(), "Bulk query should include entry for key 222");
        if (!r222.is_null()) {
            fails += !expect(r222.value("status", "") == "hit_cache", "Key 222 should be served from cache");
            fails += !expect(r222.value("found", false) == true, "Key 222 should be marked found");
        }

        auto r333 = find_result([&](const nlohmann::json& item){ return item.value("key", 0) == 333; });
        fails += !expect(!r333.is_null(), "Bulk query should include entry for key 333");
        if (!r333.is_null()) {
            fails += !expect(r333.value("status", "") == "hit_persistence", "Key 333 should be hydrated from persistence");
            fails += !expect(r333.value("value", "") == "bulk-db", "Key 333 should return persistence value");
            fails += !expect(r333.value("source", "") == "persistence", "Key 333 should indicate persistence source");
        }

        auto rTypeMismatch = find_result([&](const nlohmann::json& item){ return item.value("status", "") == "type_mismatch"; });
        fails += !expect(!rTypeMismatch.is_null(), "Bulk query should include type mismatch entry");
        if (!rTypeMismatch.is_null()) {
            fails += !expect(rTypeMismatch.value("reason", std::string()).find("integer") != std::string::npos, "Type mismatch entry should explain integer expectation");
        }

        auto r444 = find_result([&](const nlohmann::json& item){ return item.value("key", 0) == 444; });
        fails += !expect(!r444.is_null(), "Bulk query should include entry for key 444");
        if (!r444.is_null()) {
            fails += !expect(r444.value("status", "") == "miss", "Key 444 should be reported as miss");
            fails += !expect(r444.value("reason", std::string()).find("not present") != std::string::npos, "Miss entry should explain absence");
        }

        fails += !expect(body.contains("summary"), "Bulk query response should include summary");
    } else { std::cerr << "PATCH /bulk_query payload failed\n"; ++fails; }

    // invalid bulk_update payload -> 200 with errors
    if (auto res = cli.Post("/bulk_update", "{\"bad\":1}", "application/json")) {
        fails += !expect(res->status == 200, "Invalid bulk_update payload should still return 200");
        auto body = nlohmann::json::parse(res->body);
        bool sawMissingOps = false;
        if (body.contains("errors") && body["errors"].is_array()) {
            for (const auto& err : body["errors"]) {
                if (err.value("code", std::string()) == "missing_operations") {
                    sawMissingOps = true;
                    break;
                }
            }
        }
        fails += !expect(sawMissingOps, "Invalid payload should report missing_operations error");
        fails += !expect(body.value("success", true) == false, "Invalid payload should mark success false");
    } else { std::cerr << "POST /bulk_update failed\n"; ++fails; }

    // invalid bulk query payload -> still 200 with error description
    if (auto res = cli.Patch("/bulk_query", "{\"unexpected\":true}", "application/json")) {
        fails += !expect(res->status == 200, "PATCH /bulk_query invalid payload should still return 200");
        auto body = nlohmann::json::parse(res->body);
        bool sawMissing = false;
        if (body.contains("errors")) {
            for (const auto& err : body["errors"]) {
                if (err.value("code", std::string()) == "missing_data") {
                    sawMissing = true;
                    break;
                }
            }
        }
        fails += !expect(sawMissing, "Invalid payload should report missing_data error");
    } else { std::cerr << "PATCH /bulk_query invalid failed\n"; ++fails; }

    if (auto res = cli.Patch("/bulk_query", "{bad json", "application/json")) {
        fails += !expect(res->status == 200, "PATCH /bulk_query malformed JSON should still return 200");
        auto body = nlohmann::json::parse(res->body);
        bool sawParse = false;
        if (body.contains("errors")) {
            for (const auto& err : body["errors"]) {
                if (err.value("code", std::string()) == "parse_error") {
                    sawParse = true;
                    fails += !expect(err.value("reason", std::string()).find("failed to parse") != std::string::npos, "Parse error reason should be descriptive");
                    break;
                }
            }
        }
        fails += !expect(sawParse, "Malformed JSON should be captured as parse_error");
    } else { std::cerr << "PATCH /bulk_query malformed failed\n"; ++fails; }

    // 6) DELETE /delete_key/:key remove from cache -> 204
    if (auto res = cli.Delete("/delete_key/1")) {
        fails += !expect(res->status == 204, "DELETE /delete_key/1 should return 204 on success");
        fails += !expect(!fake->valueFor(1).has_value(), "Delete should remove persistence entry");
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
        fails += !expect(fake->valueFor(1).value_or("") == "new", "Update should persist new value");
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
