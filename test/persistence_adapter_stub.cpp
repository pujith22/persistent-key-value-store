#include "persistence_adapter.h"

// Minimal stub implementation used only for test builds to avoid linking libpq.
// Provide an empty definition of the private Impl so unique_ptr can be destroyed.
struct PersistenceAdapter::Impl { };
PersistenceAdapter::PersistenceAdapter(const std::string &conninfo) : p_(nullptr) {}
PersistenceAdapter::~PersistenceAdapter() {}

bool PersistenceAdapter::insert(int, const std::string&) { return true; }
bool PersistenceAdapter::update(int, const std::string&) { return true; }
bool PersistenceAdapter::remove(int) { return true; }
std::unique_ptr<std::string> PersistenceAdapter::get(int) { return nullptr; }

PersistenceAdapter::TxResult PersistenceAdapter::runTransaction(const std::vector<Operation>& ops, TxMode mode) {
    TxResult r; r.success = true; return r;
}

nlohmann::json PersistenceAdapter::runTransactionJson(const std::vector<Operation>& ops, TxMode mode) {
    nlohmann::json j; j["success"] = true; j["results"] = nlohmann::json::array();
    for (const auto &op : ops) j["results"].push_back({{"key", op.key}, {"status", "ok"}});
    return j;
}

std::future<std::unique_ptr<std::string>> PersistenceAdapter::getAsync(int key) {
    std::promise<std::unique_ptr<std::string>> p; p.set_value(nullptr);
    return p.get_future();
}

std::future<nlohmann::json> PersistenceAdapter::runTransactionJsonAsync(const std::vector<Operation>& ops, TxMode mode) {
    std::promise<nlohmann::json> p; p.set_value(runTransactionJson(ops, mode));
    return p.get_future();
}

int PersistenceAdapter::droppedPoolConnections() const { return 0; }
nlohmann::json PersistenceAdapter::poolMetrics() const { return nlohmann::json::object(); }
