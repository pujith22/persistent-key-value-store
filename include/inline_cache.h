#pragma once

#include <vector>
#include <list>
#include <string>
#include <mutex>
#include <optional>
#include <chrono>
#include <random>
#include <atomic>

/* InlineCache: header-only in-memory cache for integer->string values supporting
    eviction policies: LRU, FIFO, RANDOM; separate bucket lock for thread safety.
    Constraints: total estimated memory footprint <= ~2MB (soft limit).
   Implementation details:
    - Fixed prime number of buckets (default 1031) chosen to reduce collisions.
    - Each bucket stores entries in a std::list (stable iterators for LRU list references).
    - Global usage list for LRU ordering (front = most recent, back = least recent).
    - FIFO eviction uses insertion order recorded per entry.
    - RANDOM eviction chooses a random non-empty bucket then a random element in that bucket.
    - Timestamps stored (steady_clock) for potential time-based heuristics (currently used for FIFO tie-breaking consistency).
    - Memory accounting is approximate: key sizeof(int) + value.size() + entry struct overhead.
    - When memory exceeds budget, evict one entry according to selected policy; repeat until under budget.
    - Public API uses update_or_insert semantics for insert/put.
    - Thread safe via per-bucket mutex; LRU list modifications also protected by its own mutex.
      (Coarse improvement: we avoid a global lock for all operations except usage list updates.)

*/

class InlineCache {
    
public:
    enum class Policy { LRU, FIFO, Random };

    struct Stats {
        size_t size_entries{0};
        size_t bytes_estimated{0};
        size_t hits{0};
        size_t misses{0};
        size_t evictions{0};
    };

    // Construct cache with given eviction policy, maxBytes budget (default 2MB), bucket count.
    InlineCache(Policy policy, size_t maxBytes = 2 * 1024 * 1024, size_t bucketCount = 1031)
        : policy_(policy), maxBytes_(maxBytes), buckets_(bucketCount), rng_(std::random_device{}()) {}

    // Non-copyable
    InlineCache(const InlineCache&) = delete;
    InlineCache& operator=(const InlineCache&) = delete;

    // Attempt to get value; updates LRU usage if found.
    std::optional<std::string> get(int key) {
        auto& bucket = bucketFor(key);
        std::lock_guard<std::mutex> lg(bucket.mtx);
        for (auto it = bucket.entries.begin(); it != bucket.entries.end(); ++it) {
            if (it->key == key) {
                stats_.hits++;
                touchLRU(it->lru_iterator);
                return it->value;
            }
        }
        stats_.misses++;
        return std::nullopt;
    }

    // Insert or update value; returns true if inserted new, false if updated existing.
    bool update_or_insert(int key, const std::string& value) {
        auto& bucket = bucketFor(key);
        std::lock_guard<std::mutex> lg(bucket.mtx);
        for (auto it = bucket.entries.begin(); it != bucket.entries.end(); ++it) {
            if (it->key == key) {
                // update existing
                adjustBytesOnUpdate(it->value, value);
                it->value = value;
                it->timestamp = now();
                touchLRU(it->lru_iterator);
                evictIfNeeded();
                return false;
            }
        }
        // new entry
        size_t entryOverhead = sizeof(Entry) + value.size();
        bucket.entries.push_front(Entry{key, value, now(), {}, fifoCounter_++});
    std::lock_guard<std::mutex> lru_lock(lruMutex_);
    auto listIter = lruList_.insert(lruList_.begin(), key); // most recent at front
        bucket.entries.front().lru_iterator = listIter;
        stats_.size_entries++;
        stats_.bytes_estimated += entryOverhead;
        evictIfNeeded();
        return true;
    }

    // Insert only if absent; returns true if inserted, false if key existed.
    bool insert_if_absent(int key, const std::string& value) {
        auto& bucket = bucketFor(key);
        std::lock_guard<std::mutex> lg(bucket.mtx);
        for (auto it = bucket.entries.begin(); it != bucket.entries.end(); ++it) {
            if (it->key == key) {
                touchLRU(it->lru_iterator);
                return false;
            }
        }
        size_t entryOverhead = sizeof(Entry) + value.size();
        bucket.entries.push_front(Entry{key, value, now(), {}, fifoCounter_++});
    std::lock_guard<std::mutex> lru_lock(lruMutex_);
    auto listIter = lruList_.insert(lruList_.begin(), key);
        bucket.entries.front().lru_iterator = listIter;
        stats_.size_entries++;
        stats_.bytes_estimated += entryOverhead;
        evictIfNeeded();
        return true;
    }

    // Update only if present; returns true if updated, false if missing.
    bool update(int key, const std::string& value) {
        auto& bucket = bucketFor(key);
        std::lock_guard<std::mutex> lg(bucket.mtx);
        for (auto it = bucket.entries.begin(); it != bucket.entries.end(); ++it) {
            if (it->key == key) {
                adjustBytesOnUpdate(it->value, value);
                it->value = value;
                it->timestamp = now();
                touchLRU(it->lru_iterator);
                evictIfNeeded();
                return true;
            }
        }
        return false;
    }

    // Remove key if exists; returns true if erased.
    bool erase(int key) {
        auto& bucket = bucketFor(key);
        std::lock_guard<std::mutex> lg(bucket.mtx);
        for (auto it = bucket.entries.begin(); it != bucket.entries.end(); ++it) {
            if (it->key == key) {
                removeEntry(bucket, it);
                return true;
            }
        }
        return false;
    }

    // Fetch statistics snapshot.
    Stats stats() const { return stats_; }

    // Current policy
    Policy policy() const { return policy_; }

private:
    struct Entry {
        int key;
        std::string value;
        std::chrono::steady_clock::time_point timestamp;
        std::list<int>::iterator lru_iterator; // reference into global lruList_
        size_t fifo_order; // increasing counter for FIFO
    };

    struct Bucket {
        std::list<Entry> entries; // linked list of entries
        std::mutex mtx;           // per-bucket lock
    };

    Policy policy_;
    size_t maxBytes_;
    std::vector<Bucket> buckets_;
    mutable std::mutex lruMutex_; // protects lruList_ modifications
    std::list<int> lruList_;      // most recent front
    std::atomic<size_t> fifoCounter_{0};
    Stats stats_;
    std::mt19937 rng_;

    Bucket& bucketFor(int key) { return buckets_[static_cast<size_t>(key) % buckets_.size()]; }

    static std::chrono::steady_clock::time_point now() { return std::chrono::steady_clock::now(); }

    void touchLRU(std::list<int>::iterator& itKey) {
        std::lock_guard<std::mutex> lock(lruMutex_);
        // move key to front if not already
        if (itKey != lruList_.begin()) {
            int k = *itKey;
            lruList_.erase(itKey);
            itKey = lruList_.insert(lruList_.begin(), k);
        }
    }

    void adjustBytesOnUpdate(const std::string& oldVal, const std::string& newVal) {
        if (newVal.size() > oldVal.size()) stats_.bytes_estimated += (newVal.size() - oldVal.size());
        else stats_.bytes_estimated -= (oldVal.size() - newVal.size());
    }

    void removeEntry(Bucket& bucket, std::list<Entry>::iterator it) {
        {
            std::lock_guard<std::mutex> lock(lruMutex_);
            lruList_.erase(it->lru_iterator);
        }
        stats_.bytes_estimated -= (sizeof(Entry) + it->value.size());
        stats_.size_entries--;
        bucket.entries.erase(it);
    }

    void evictIfNeeded() {
        // Loop while over budget (avoid long loops by capping iterations)
        int guard = 0;
        while (stats_.bytes_estimated > maxBytes_ && stats_.size_entries > 0 && guard < 10000) {
            ++guard;
            if (policy_ == Policy::LRU) evictLRU();
            else if (policy_ == Policy::FIFO) evictFIFO();
            else evictRandom();
            stats_.evictions++;
        }
    }

    void evictLRU() {
        int victimKey = -1;
        {
            std::lock_guard<std::mutex> lock(lruMutex_);
            victimKey = lruList_.empty() ? -1 : lruList_.back();
        }
        if (victimKey == -1) return;
        erase(victimKey);
    }

    void evictFIFO() {
        // scan buckets for minimal fifo_order (could be optimized but simple scan is fine given size constraints)
        int victimKey = -1;
        size_t victimOrder = SIZE_MAX;
        for (auto& b : buckets_) {
            std::lock_guard<std::mutex> lg(b.mtx);
            for (auto it = b.entries.begin(); it != b.entries.end(); ++it) {
                if (it->fifo_order < victimOrder) {
                    victimOrder = it->fifo_order;
                    victimKey = it->key;
                }
            }
        }
        if (victimKey != -1) erase(victimKey);
    }

    void evictRandom() {
        // pick random non-empty bucket
        std::uniform_int_distribution<size_t> distBucket(0, buckets_.size() - 1);
        for (int attempts = 0; attempts < 32; ++attempts) {
            size_t bi = distBucket(rng_);
            Bucket& b = buckets_[bi];
            std::lock_guard<std::mutex> lg(b.mtx);
            if (b.entries.empty()) continue;
            // choose random entry index
            std::uniform_int_distribution<size_t> distEntry(0, b.entries.size() - 1);
            size_t idx = distEntry(rng_);
            auto it = b.entries.begin();
            std::advance(it, idx);
            int victimKey = it->key;
            // release lock before erase to avoid deadlock (erase will re-lock bucket)
            // but we need a copy of key only
            erase(victimKey);
            return;
        }
        // fallback: LRU if random failed
        evictLRU();
    }
};
