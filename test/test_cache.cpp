#include "inline_cache.h"
#include <iostream>
#include <string>
#include <vector>
#include <thread>
#include <chrono>

static bool expect(bool cond, const char* msg) {
    if (!cond) std::cerr << "ASSERT FAILED: " << msg << "\n";
    return cond;
}

// Helper: estimate bytes for one entry storing given value (approximate internal accounting)
static size_t estimate_entry_bytes(const std::string& value) {
    InlineCache tmp{InlineCache::Policy::LRU, 10 * 1024 * 1024};
    tmp.upsert(1, value);
    auto st = tmp.stats();
    return st.bytes_estimated;
}

int main() {
    int failures = 0;

    // Basic operations
    {
        InlineCache cache{InlineCache::Policy::LRU};
        // insert if absent
        failures += !expect(cache.insert_if_absent(10, "a"), "insert_if_absent should insert new");
        failures += !expect(!cache.insert_if_absent(10, "b"), "insert_if_absent should not overwrite existing");
        // get
        auto v = cache.get(10);
        failures += !expect(v && *v == "a", "get should see original value after failed insert_if_absent");
        // update
        failures += !expect(cache.update(10, "c"), "update existing should succeed");
        v = cache.get(10);
        failures += !expect(v && *v == "c", "get should see updated value");
        // erase
        failures += !expect(cache.erase(10), "erase existing should succeed");
        failures += !expect(!cache.get(10), "get after erase should miss");
    }

    // LRU eviction: capacity for two entries, insert three -> first (LRU) should be evicted
    {
        std::string val = "x"; // keep value consistent across entries
        size_t per = estimate_entry_bytes(val);
        // capacity for 2 entries (approx), allow small margin
        InlineCache cache{InlineCache::Policy::LRU, per * 2 + 16};
        cache.upsert(1, val);
        cache.upsert(2, val);
        // Touch key 1 to make it MRU so key 2 becomes LRU
        (void)cache.get(1);
        cache.upsert(3, val); // triggers eviction of key 2 if eviction happens
        // We cannot guarantee exact eviction count, but LRU victim should be the least-recently-used
        auto v1 = cache.get(1);
        auto v2 = cache.get(2);
        auto v3 = cache.get(3);
        // Expect 1 and 3 present, 2 possibly evicted
        failures += !expect(v1.has_value(), "LRU: key 1 should be present (MRU)");
        failures += !expect(v3.has_value(), "LRU: key 3 should be present");
        // If an eviction occurred, key 2 is the likely victim
        if (cache.stats().evictions > 0) {
            failures += !expect(!v2.has_value(), "LRU: key 2 should be evicted when over budget");
        }
    }

    // FIFO eviction: capacity for two entries, insert three -> first inserted should go
    {
        std::string val = "y";
        size_t per = estimate_entry_bytes(val);
        InlineCache cache{InlineCache::Policy::FIFO, per * 2 + 16};
        cache.upsert(11, val); // first inserted
        cache.upsert(12, val);
        cache.upsert(13, val); // should evict 11 under FIFO if over budget
        auto v11 = cache.get(11);
        auto v12 = cache.get(12);
        auto v13 = cache.get(13);
        if (cache.stats().evictions > 0) {
            failures += !expect(!v11.has_value(), "FIFO: oldest key should be evicted");
            failures += !expect(v12.has_value() && v13.has_value(), "FIFO: later keys should remain");
        } else {
            // No eviction happened (budget generous); still all keys should be present
            failures += !expect(v11.has_value() && v12.has_value() && v13.has_value(), "FIFO: all present when no eviction");
        }
    }

    // RANDOM eviction: ensure evictions occur and size bounded
    {
        std::string val(32, 'z');
        size_t per = estimate_entry_bytes(val);
        InlineCache cache{InlineCache::Policy::Random, per * 4 + 16}; // ~4 entries capacity
        for (int k = 100; k < 120; ++k) cache.upsert(k, val);
        auto st = cache.stats();
        failures += !expect(st.evictions > 0, "Random: expected at least one eviction when over budget");
        // After evictions, should not exceed budget
        failures += !expect(st.bytes_estimated <= (per * 4 + 16), "Random: bytes should be <= budget");
    }

    // Concurrency smoke test: multiple threads upserting disjoint key ranges
    {
        InlineCache cache{InlineCache::Policy::LRU};
        auto worker = [&cache](int start){
            for (int i = 0; i < 100; ++i) cache.upsert(start + i, std::to_string(start + i));
        };
        std::thread t1(worker, 0);
        std::thread t2(worker, 1000);
        t1.join(); t2.join();
        // spot check
        failures += !expect(cache.get(5).has_value(), "Concurrency: key 5 present");
        failures += !expect(cache.get(1005).has_value(), "Concurrency: key 1005 present");
    }

    if (failures == 0) {
        std::cout << "All InlineCache tests passed." << std::endl;
        return 0;
    }
    std::cerr << failures << " InlineCache test(s) failed." << std::endl;
    return 1;
}
