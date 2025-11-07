#include "persistence_adapter.h"
#include "config.h"
#include <iostream>
#include <vector>
#include <functional>
#include "nlohmann/json.hpp"

// Tiny assert helper
static bool expect_true(bool cond, const char* msg) {
    if (!cond) std::cerr << "ASSERT FAILED: " << msg << "\n";
    return cond;
}

int main()
{
    // Connection string from env PG_CONNINFO or config/db.json, fallback to dbname=kvstore
    const std::string conninfo = load_conninfo();

    try {
        PersistenceAdapter db{conninfo};

        // ---------------- Basic CRUD coverage ----------------
        std::cout << "[CRUD] Prepare clean keys...\n";
        db.remove(10); db.remove(11);

        std::cout << "[CRUD] insert new key=10 -> 'hello'\n";
        if (!expect_true(db.insert(10, "hello"), "insert key 10 should succeed")) return 1;
        auto v = db.get(10);
        if (!expect_true(v && *v == "hello", "get key 10 should be 'hello'")) return 1;

        std::cout << "[CRUD] insert same key=10 -> 'hello2' (upsert should update)\n";
        if (!expect_true(db.insert(10, "hello2"), "upsert insert should succeed")) return 1;
        v = db.get(10);
        if (!expect_true(v && *v == "hello2", "get after upsert should be 'hello2'")) return 1;

        std::cout << "[CRUD] update existing key=10 -> 'world'\n";
        if (!expect_true(db.update(10, "world"), "update existing should return true")) return 1;
        v = db.get(10);
        if (!expect_true(v && *v == "world", "value should be 'world' after update")) return 1;

        std::cout << "[CRUD] update missing key=999 -> returns false\n";
        if (!expect_true(!db.update(999, "noop"), "update missing should be false")) return 1;

        std::cout << "[CRUD] remove existing key=10 -> true, remove again -> false\n";
        if (!expect_true(db.remove(10), "remove existing should be true")) return 1;
        if (!expect_true(!db.remove(10), "remove missing should be false")) return 1;
        v = db.get(10);
        if (!expect_true(!v, "get removed key should be null")) return 1;

        // ---------------- Transaction (TxResult) coverage ----------------
        std::cout << "[TX] Silent mode with mixed ops (no Get in TxResult API)\n";
        db.remove(10);
        std::vector<PersistenceAdapter::Operation> ops_silent = {
            {PersistenceAdapter::OpType::Insert, 10, "a"},
            {PersistenceAdapter::OpType::Update, 999, "x"}, // fail (no rows)
            {PersistenceAdapter::OpType::Update, 10,  "b"}, // success
            {PersistenceAdapter::OpType::Remove, 999, ""},  // fail (no rows)
            {PersistenceAdapter::OpType::Remove, 10,  ""}   // success
        };
        auto txres1 = db.runTransaction(ops_silent, PersistenceAdapter::TxMode::Silent);
        if (!expect_true(txres1.success, "Silent txn should commit despite failures")) return 1;
        if (!expect_true(txres1.failures.size() == 2, "Silent txn should record exactly 2 failures (update 999, remove 999)")) return 1;
        if (!expect_true(!db.get(10), "Post Silent txn: key 10 should be absent")) return 1;

        std::cout << "[TX] RollbackOnError with mid failure; ensure no effects\n";
        std::vector<PersistenceAdapter::Operation> ops_rb = {
            {PersistenceAdapter::OpType::Insert, 10, "c"},
            {PersistenceAdapter::OpType::Update, 999, "x"}, // failure causes rollback
            {PersistenceAdapter::OpType::Remove, 10,  ""}
        };
        auto txres2 = db.runTransaction(ops_rb, PersistenceAdapter::TxMode::RollbackOnError);
        if (!expect_true(!txres2.success, "Rollback txn should report failure")) return 1;
        if (!expect_true(!db.get(10), "Post Rollback txn: key 10 must not exist")) return 1;

        // ---------------- JSON Transaction coverage: Silent mode ----------------
        std::cout << "[JSON] Silent mode combinations with get/insert/update/remove\n";
        // Keys used: 201,202,203
        db.remove(201); db.remove(202); db.remove(203);
        std::vector<PersistenceAdapter::Operation> js_ops_silent = {
            {PersistenceAdapter::OpType::Insert, 201, "a"}, // 0 ok
            {PersistenceAdapter::OpType::Get,    201, ""},  // 1 ok -> "a"
            {PersistenceAdapter::OpType::Update, 202, "x"}, // 2 fail -> no rows
            {PersistenceAdapter::OpType::Get,    202, ""},  // 3 ok -> null
            {PersistenceAdapter::OpType::Update, 201, "b"}, // 4 ok
            {PersistenceAdapter::OpType::Get,    201, ""},  // 5 ok -> "b"
            {PersistenceAdapter::OpType::Remove, 202, ""},  // 6 fail -> no rows
            {PersistenceAdapter::OpType::Remove, 201, ""},  // 7 ok
            {PersistenceAdapter::OpType::Get,    201, ""},  // 8 ok -> null
            {PersistenceAdapter::OpType::Insert, 203, "c"}, // 9 ok
            {PersistenceAdapter::OpType::Insert, 203, "d"}, // 10 ok (upsert)
            {PersistenceAdapter::OpType::Get,    203, ""}   // 11 ok -> "d"
        };
        auto js1 = db.runTransactionJson(js_ops_silent, PersistenceAdapter::TxMode::Silent);
        if (!expect_true(js1.contains("mode") && js1["mode"] == "silent", "JSON mode must be silent")) return 1;
        if (!expect_true(js1.contains("success") && js1["success"].get<bool>() == true, "JSON Silent success should be true")) return 1;
        if (!expect_true(js1.contains("results") && js1["results"].is_array() && js1["results"].size() == js_ops_silent.size(), "JSON results length must match ops")) return 1;
        // Verify selected indices in order
        auto get_item = [&](size_t i){ return js1["results"][i]; };
        if (!expect_true(get_item(1)["op"]=="get" && get_item(1)["key"]==201 && get_item(1)["status"]=="ok" && get_item(1)["value"]=="a", "get(201) after insert should be 'a'")) return 1;
        if (!expect_true(get_item(2)["status"]=="failed" && get_item(2)["error"]=="no rows affected", "update(202) should fail with no rows affected")) return 1;
        if (!expect_true(get_item(3)["op"]=="get" && get_item(3)["value"].is_null(), "get(202) should return null")) return 1;
        if (!expect_true(get_item(5)["op"]=="get" && get_item(5)["value"]=="b", "get(201) after update should be 'b'")) return 1;
        if (!expect_true(get_item(6)["status"]=="failed" && get_item(6)["error"]=="no rows affected", "remove(202) should fail")) return 1;
        if (!expect_true(get_item(8)["op"]=="get" && get_item(8)["value"].is_null(), "get(201) after remove should be null")) return 1;
        if (!expect_true(get_item(11)["op"]=="get" && get_item(11)["value"]=="d", "get(203) after upsert should be 'd'")) return 1;
        // State after commit
        if (!expect_true(!db.get(201), "post JSON silent: 201 should be absent")) return 1;
        auto v203 = db.get(203);
        if (!expect_true(v203 && *v203=="d", "post JSON silent: 203 should be 'd'")) return 1;

        // ---------------- JSON Transaction coverage: Rollback mode ----------------
        std::cout << "[JSON] Rollback mode with early failure and gets\n";
        db.remove(204); db.remove(205);
        std::vector<PersistenceAdapter::Operation> js_ops_rb = {
            {PersistenceAdapter::OpType::Insert, 204, "m"}, // ok
            {PersistenceAdapter::OpType::Get,    204, ""},  // ok -> "m"
            {PersistenceAdapter::OpType::Remove, 205, ""},  // fail -> no rows (triggers rollback)
            {PersistenceAdapter::OpType::Insert, 205, "n"}  // should not execute
        };
        auto js2 = db.runTransactionJson(js_ops_rb, PersistenceAdapter::TxMode::RollbackOnError);
        if (!expect_true(js2.contains("mode") && js2["mode"]=="rollback", "JSON mode must be rollback")) return 1;
        if (!expect_true(js2.contains("success") && js2["success"].get<bool>()==false, "JSON rollback success should be false")) return 1;
        if (!expect_true(js2["results"].size()==3, "JSON rollback should stop at failing op")) return 1;
        if (!expect_true(js2["results"][1]["op"]=="get" && js2["results"][1]["value"]=="m", "get(204) before failure should see 'm'")) return 1;
        // Ensure nothing persisted
        if (!expect_true(!db.get(204) && !db.get(205), "post JSON rollback: no keys should exist")) return 1;

        // ---------------- In-transaction read-after-write semantics (Silent) ----------------
        std::cout << "[JSON] Silent: read-your-writes and read-after-delete\n";
        db.remove(206);
        std::vector<PersistenceAdapter::Operation> js_ops_rw = {
            {PersistenceAdapter::OpType::Insert, 206, "x"},
            {PersistenceAdapter::OpType::Get,    206, ""},   // expect "x"
            {PersistenceAdapter::OpType::Remove, 206, ""},
            {PersistenceAdapter::OpType::Get,    206, ""},   // expect null
            {PersistenceAdapter::OpType::Update, 9999, "z"}  // fail, but prior ops remain
        };
        auto js3 = db.runTransactionJson(js_ops_rw, PersistenceAdapter::TxMode::Silent);
        if (!expect_true(js3["results"][1]["value"]=="x", "read-your-writes value 'x'")) return 1;
        if (!expect_true(js3["results"][3]["value"].is_null(), "read-after-delete should be null")) return 1;
        if (!expect_true(js3["success"].get<bool>()==true, "silent with failures still success true")) return 1;
        if (!expect_true(!db.get(206), "post JSON silent: 206 removed")) return 1;

        std::cout << "All tests passed.\n";
        return 0;
    } catch (const std::exception &e) {
        std::cerr << "Fatal: " << e.what() << '\n';
        return 2;
    }
}
