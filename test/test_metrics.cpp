#include "server.h"
#include <httplib.h>
#include <nlohmann/json.hpp>
#include <iostream>
#include <thread>
#include <chrono>
#include <optional>

using namespace std::chrono_literals;

static bool expect(bool cond, const char* msg) {
    if (!cond) std::cerr << "ASSERT FAILED: " << msg << "\n";
    return cond;
}

struct FakePersistence : PersistenceProvider {
    bool insert(int, const std::string&) override { return true; }
    bool update(int, const std::string&) override { return true; }
    bool remove(int) override { return true; }
    std::unique_ptr<std::string> get(int) override { return nullptr; }
};

static bool wait_until_up(const std::string& host, int port, int retries = 100, int ms = 20) {
    httplib::Client cli(host, port);
    cli.set_connection_timeout(0, 200000);
    for (int i = 0; i < retries; ++i) {
        if (auto res = cli.Get("/")) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(ms));
    }
    return false;
}

int main() {
    const std::string host = "localhost";
    const int port = 23877; // test port

    KeyValueServer server{host, port};
    auto fake = std::make_unique<FakePersistence>();
    server.setPersistenceProvider(std::move(fake), "test-metrics");
    server.setupRoutes();

    std::thread srv([&]{ server.start(); });
    if (!wait_until_up(host, port)) {
        std::cerr << "Server did not start" << std::endl;
        if (srv.joinable()) server.stop(), srv.join();
        return 2;
    }

    httplib::Client cli(host, port);
    cli.set_connection_timeout(0, 500000);

    // call metrics twice to allow per-second deltas to be computed
    if (auto r1 = cli.Get("/metrics")) {
        if (r1->status != 200) { std::cerr << "/metrics first call status " << r1->status << std::endl; }
    } else { std::cerr << "First /metrics call failed" << std::endl; }
    std::this_thread::sleep_for(1100ms);
    int fails = 0;
    if (auto res = cli.Get("/metrics")) {
        if (res->status != 200) {
            std::cerr << "/metrics returned status " << res->status << std::endl;
            ++fails;
        } else {
            auto body = nlohmann::json::parse(res->body);
            // check CPU
            fails += !expect(body.contains("cpu_utilization_percent"), "metrics should include cpu_utilization_percent");
            // memory
            fails += !expect(body.contains("memory_kb") && body["memory_kb"].is_object(), "metrics should include memory_kb object");
            // disk
            fails += !expect(body.contains("disk_read_bytes"), "metrics should include disk_read_bytes");
            fails += !expect(body.contains("disk_read_bytes_per_sec"), "metrics should include disk_read_bytes_per_sec");
            // network
            fails += !expect(body.contains("network_rx_bytes_per_sec"), "metrics should include network_rx_bytes_per_sec");
            // disk util
            fails += !expect(body.contains("disk_utilization_percent"), "metrics should include disk_utilization_percent");
            // process level
            fails += !expect(body.contains("process") && body["process"].is_object(), "metrics should include process object");
            if (body.contains("process")) {
                auto p = body["process"];
                fails += !expect(p.contains("rss_kb") && p.contains("vms_kb"), "process should include rss_kb and vms_kb");
                fails += !expect(p.contains("threads") && p.contains("open_fds"), "process should include threads and open_fds");
            }
        }
    } else { std::cerr << "GET /metrics failed" << std::endl; ++fails; }

    // stop server
    if (auto s = cli.Get("/stop")) { std::this_thread::sleep_for(200ms); }
    if (srv.joinable()) srv.join();

    if (fails == 0) {
        std::cout << "Metrics test passed" << std::endl;
        return 0;
    }
    std::cerr << fails << " metrics test(s) failed" << std::endl;
    return 1;
}
