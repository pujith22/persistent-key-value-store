// Refactored implementation: KeyValueServer class defined in server.h
#include "server.h"
#include <iostream>
#include <chrono>
#include <sstream>
#include <fstream>
#include <optional>
#include <utility>
#include <algorithm>
#include <cctype>
#include <functional>
#include <thread>
#include <mutex>
#include <map>
#include <fstream>
#include <filesystem>

const std::vector<KeyValueServer::RouteDescriptor> KeyValueServer::routes_json = {
    {"GET", "/", "Machine-readable service catalog"},
    {"GET", "/home", "Formatted documentation for available routes"},
    {"GET", "/get_key/:key_id", "Return the value for the provided numeric key caching it if not present in cache"},
    {"PATCH", "/bulk_query", "Retrieve multiple keys in one request; missing keys noted in response, always return success response with error appended to the response"},
    {"POST", "/insert/:key/:value", "Insert a key/value pair; conflicts return 409 with existing value, writes both to cache and persistence layer (note that we are using write-through type of cache)"},
    {"POST", "/bulk_update", "Transactional Commit pipeline for create/get/insert/update operations, rollbacks in case of failure and retuns failure response"},
    {"DELETE", "/delete_key/:key", "Remove the provided key from both the cache and persistence layer"},
    {"PUT", "/update_key/:key/:value", "Update an existing key with a new value to both the cache and persistence layer"},
    {"GET", "/health", "Report service health and uptime"},
    {"GET", "/metrics", "Expose cache metrics including hit/miss counts"},
    {"GET", "/stop", "Gracefully stop the server (testing/debug only), shouldn't be available in prod environment"}
};

KeyValueServer::KeyValueServer(const std::string& host, int port, InlineCache::Policy policy, bool json_logging)
    : host_(host), port_(port), inline_cache(policy, 1ULL * 1024 * 1024 * 1024), json_logging_enabled(json_logging) {
    server_boot_time = std::chrono::steady_clock::now();
}

KeyValueServer::~KeyValueServer() = default;

void KeyValueServer::logRequest(const httplib::Request& req) {
    if (!logging_enabled) return;
    if (json_logging_enabled) {
        nlohmann::json j{{"type","request"},{"method",req.method},{"path",req.path}};
        // include path params, query params and body size for richer diagnostics
        if (!req.path_params.empty()) j["path_params"] = req.path_params;
        if (!req.params.empty()) {
            nlohmann::json qp;
            for (const auto &p : req.params) qp[p.first] = p.second;
            j["query_params"] = qp;
        }
        j["path_param_count"] = req.path_params.size();
        j["body_bytes"] = req.body.size();
        std::cout << j.dump() << std::endl;
    } else {
        std::cout << "[REQUEST] method=" << req.method << " path=" << req.path << "\n";
    }
}

void KeyValueServer::logResponse(const httplib::Response& res, std::chrono::steady_clock::duration duration) {
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
    if (!logging_enabled) return;
    if (json_logging_enabled) {
        nlohmann::json j{{"type","response"},{"status",res.status},{"reason",res.reason},
                         {"content_type",res.get_header_value("Content-Type")},{"bytes",res.body.size()},
                         {"duration_ms",ms}};
        // If the response body looks like JSON, attempt to include any 'reason' field present for observability
        try {
            if (!res.body.empty()) {
                auto parsed = nlohmann::json::parse(res.body);
                if (parsed.is_object() && parsed.contains("reason")) j["reason_detail"] = parsed["reason"];
                else if (parsed.is_object() && parsed.contains("error")) j["error_detail"] = parsed["error"];
            }
        } catch (...) {
            // ignore parse errors
        }
        std::cout << j.dump() << std::endl;
    } else {
        std::cout << "[RESPONSE] status=" << res.status << " reason=" << res.reason
                  << " ct=" << res.get_header_value("Content-Type")
                  << " bytes=" << res.body.size()
                  << " duration_ms=" << ms << "\n";
    }
}

// Validate that the request contains exactly the expected path parameters (by name).
static bool validate_path_params(const httplib::Request& req, const std::vector<std::string>& expected, nlohmann::json &out, std::string &reason_msg) {
    // check count
    if (req.path_params.size() != expected.size()) {
        std::ostringstream ss; ss << "expected " << expected.size() << " path params but got " << req.path_params.size();
        reason_msg = ss.str();
        out["error"] = "invalid_path_params";
        out["reason"] = reason_msg;
        if (!req.path_params.empty()) out["provided_path_params"] = req.path_params;
        return false;
    }
    // check presence of keys
    for (const auto &k : expected) {
        if (!req.path_params.count(k)) {
            std::ostringstream ss; ss << "missing path param '" << k << "'";
            reason_msg = ss.str();
            out["error"] = "invalid_path_params";
            out["reason"] = reason_msg;
            if (!req.path_params.empty()) out["provided_path_params"] = req.path_params;
            return false;
        }
    }
    return true;
}

void KeyValueServer::homeHandler(const httplib::Request& req, httplib::Response& res) {
    auto start = std::chrono::steady_clock::now();
    logRequest(req);

    std::string html = renderHomePage();
    res.status = 200;
    res.reason = "ok";
    res.set_content(html, "text/html; charset=utf-8");
    logResponse(res, std::chrono::steady_clock::now() - start);
}

void KeyValueServer::indexHandler(const httplib::Request& req, httplib::Response& res) {
    auto start = std::chrono::steady_clock::now();
    logRequest(req);

    nlohmann::json payload;
    payload["service"] = "Persistent Key Value Store";
    payload["version"] = "1.0";
    payload["description"] = "HTTP-accessible cache with inline persistence adapter hooks";

    nlohmann::json routes = nlohmann::json::array();
    for (const auto& route : routes_json) {
        routes.push_back({
            {"method", route.method},
            {"path", route.path},
            {"description", route.description}
        });
    }
    payload["routes"] = routes;
    payload["links"] = {
        {"home", "/home"},
        {"health", "/health"},
        {"metrics", "/metrics"}
    };

    json_response(res, 200, payload, "ok");
    logResponse(res, std::chrono::steady_clock::now() - start);
}

std::string KeyValueServer::renderHomePage() const {
    std::ifstream file(home_page_template_path);
    if (!file.is_open()) {
        std::ostringstream fallback;
        fallback << "<!DOCTYPE html><html lang=\"en\"><head><meta charset=\"utf-8\"><title>Persistent Key Value Store</title>"
                 << "<style>body{font-family:Arial,sans-serif;padding:2rem;background:#f6f8fb;color:#1b1f23;}"
                 << "h1{color:#24292e;} ul{padding-left:1.2rem;} li{margin-bottom:0.4rem;}</style></head><body>"
                 << "<h1>Persistent Key Value Store</h1><p>Static homepage template not found at '<code>" << home_page_template_path
                 << "</code>'. Rendering minimal fallback.</p><h2>Available Routes</h2><ul>";
        for (const auto& route : routes_json) {
            fallback << "<li><strong>" << route.method << "</strong> <code>" << route.path << "</code> &mdash; "
                     << route.description << "</li>";
        }
        fallback << "</ul></body></html>";
        return fallback.str();
    }

    std::ostringstream buffer;
    buffer << file.rdbuf();
    std::string html = buffer.str();

    auto replace_all = [&](const std::string& placeholder, const std::string& value) {
        std::string::size_type pos = 0;
        while ((pos = html.find(placeholder, pos)) != std::string::npos) {
            html.replace(pos, placeholder.size(), value);
            pos += value.size();
        }
    };

    std::ostringstream rows;
    for (const auto& route : routes_json) {
        rows << "<tr><td class=\"route-method\">" << route.method << "</td><td><code>" << route.path
             << "</code></td><td>" << route.description << "</td></tr>";
    }

    replace_all("{{ROUTE_ROWS}}", rows.str());
    replace_all("{{SERVICE_NAME}}", "Persistent Key Value Store");
    replace_all("{{SERVICE_TAGLINE}}", "with in-memory cache with complete observability");
    replace_all("{{JSON_ENDPOINT}}", "/");

    return html;
}

void KeyValueServer::getKeyHandler(const httplib::Request& req, httplib::Response& res) {
    auto start = std::chrono::steady_clock::now();
    logRequest(req);
    std::string id;
    nlohmann::json out;
    std::string reason_msg;
    if (!validate_path_params(req, {"key_id"}, out, reason_msg)) {
        json_response(res, 400, out, reason_msg.c_str());
        logResponse(res, std::chrono::steady_clock::now() - start);
        return;
    }
    if (req.has_param("key_id")) id = req.get_param_value("key_id");
    else if (req.path_params.count("key_id")) id = req.path_params.at("key_id");
    int key;
    out["query_key"] = id;
    if (!parse_int(id, key)) {
        out["error"] = "invalid key format";
        out["reason"] = "path parameter 'key_id' must be an integer";
        json_response(res, 400, out, "invalid_key_format");
        logResponse(res, std::chrono::steady_clock::now() - start);
        return;
    }
    auto v = inline_cache.get(key);
    if (v) {
        out["found"] = true;
        out["value"] = *v;
        json_response(res, 200, out, "ok");
    } else {
        bool persistence_checked = false;
        if (persistence_adapter) {
            persistence_checked = true;
            // if underlying adapter supports async get, offload DB work to its worker pool
            if (auto* ada = dynamic_cast<PersistenceAdapter*>(persistence_adapter.get())) {
                try {
                    auto fut = ada->getAsync(key);
                    auto persisted = fut.get();
                    if (persisted) {
                        out["found"] = true;
                        out["value"] = *persisted;
                        out["source"] = "persistence";
                        bool inserted_cache = inline_cache.update_or_insert(key, *persisted);
                        out["cache_populated"] = inserted_cache;
                        json_response(res, 200, out, "ok");
                        logResponse(res, std::chrono::steady_clock::now() - start);
                        return;
                    }
                } catch (...) {
                    // fall back to synchronous call below
                }
            } else {
                if (auto persisted = persistence_adapter->get(key)) {
                    out["found"] = true;
                    out["value"] = *persisted;
                    out["source"] = "persistence";
                    bool inserted_cache = inline_cache.update_or_insert(key, *persisted);
                    out["cache_populated"] = inserted_cache;
                    json_response(res, 200, out, "ok");
                    logResponse(res, std::chrono::steady_clock::now() - start);
                    return;
                }
            }
        }
        out["found"] = false;
        out["reason"] = persistence_checked ? "key not present in cache or persistence" : "key not present in cache";
        out["persistence_checked"] = persistence_checked;
        json_response(res, 404, out, "not_found");
    }
    logResponse(res, std::chrono::steady_clock::now() - start);
}

void KeyValueServer::bulkQueryHandler(const httplib::Request& req, httplib::Response& res) {
    auto start = std::chrono::steady_clock::now();
    logRequest(req);
    nlohmann::json out;
    out["endpoint"] = "bulk_query";
    nlohmann::json results = nlohmann::json::array();
    nlohmann::json errors = nlohmann::json::array();

    auto push_error = [&](const std::string& code, const std::string& reason, nlohmann::json detail = nlohmann::json()) {
        nlohmann::json err{{"code", code}, {"reason", reason}};
        if (!detail.is_null()) {
            err["detail"] = std::move(detail);
        }
        errors.push_back(err);
    };

    size_t hit_cache = 0, hit_persistence = 0, miss = 0, type_mismatch = 0;

    if (req.body.empty()) {
        push_error("empty_body", "request body must include a JSON object with a 'data' array of integer keys");
    } else {
        try {
            auto payload = nlohmann::json::parse(req.body);
            if (!payload.is_object()) {
                push_error("invalid_payload", "JSON body must be an object containing a 'data' array");
            } else if (!payload.contains("data")) {
                push_error("missing_data", "JSON object must contain a 'data' key mapped to an array");
            } else if (!payload["data"].is_array()) {
                nlohmann::json detail{{"provided_type", payload["data"].type_name()}};
                push_error("invalid_data_type", "'data' must be an array of integers", detail);
            } else {
                const auto& data = payload["data"];
                for (size_t idx = 0; idx < data.size(); ++idx) {
                    const auto& el = data[idx];
                    nlohmann::json item;
                    item["index"] = idx;
                    item["input"] = el;

                    if (!el.is_number_integer()) {
                        item["status"] = "type_mismatch";
                        item["found"] = false;
                        item["reason"] = "expected integer key";
                        item["provided_type"] = el.type_name();
                        results.push_back(item);
                        ++type_mismatch;
                        continue;
                    }

                    int key = el.get<int>();
                    item["key"] = key;
                    if (auto cached = inline_cache.get(key)) {
                        item["status"] = "hit_cache";
                        item["found"] = true;
                        item["value"] = *cached;
                        item["source"] = "cache";
                        item["reason"] = "value served from cache";
                        ++hit_cache;
                    } else {
                        bool persistence_checked = false;
                        if (persistence_adapter) {
                            persistence_checked = true;
                            if (auto persisted = persistence_adapter->get(key)) {
                                inline_cache.update_or_insert(key, *persisted);
                                item["status"] = "hit_persistence";
                                item["found"] = true;
                                item["value"] = *persisted;
                                item["source"] = "persistence";
                                item["reason"] = "value hydrated from persistence";
                                item["cache_populated"] = true;
                                ++hit_persistence;
                            } else {
                                item["status"] = "miss";
                                item["found"] = false;
                                item["value"] = nullptr;
                                item["reason"] = "key not present in cache or persistence";
                                ++miss;
                            }
                        } else {
                            item["status"] = "miss";
                            item["found"] = false;
                            item["value"] = nullptr;
                            item["reason"] = "key not present in cache";
                            ++miss;
                        }
                        if (persistence_adapter) {
                            item["persistence_checked"] = true;
                        }
                    }

                    results.push_back(item);
                }
            }
        } catch (const std::exception& e) {
            push_error("parse_error", std::string("failed to parse request JSON: ") + e.what());
        }
    }

    out["results"] = results;
    if (!errors.empty()) {
        out["errors"] = errors;
    }

    nlohmann::json summary;
    summary["requested"] = results.size();
    summary["hit_cache"] = hit_cache;
    summary["hit_persistence"] = hit_persistence;
    summary["misses"] = miss;
    summary["type_mismatch"] = type_mismatch;
    summary["top_level_errors"] = errors.size();
    out["summary"] = summary;
    out["success"] = errors.empty();

    json_response(res, 200, out, "ok");
    logResponse(res, std::chrono::steady_clock::now() - start);
}

void KeyValueServer::insertionHandler(const httplib::Request& req, httplib::Response& res) {
    auto start = std::chrono::steady_clock::now();
    logRequest(req);
    std::string key_str, value_str;
    nlohmann::json out;
    std::string reason_msg;
    if (!validate_path_params(req, {"key","value"}, out, reason_msg)) {
        json_response(res, 400, out, reason_msg.c_str());
        logResponse(res, std::chrono::steady_clock::now() - start);
        return;
    }
    if (req.path_params.count("key")) key_str = req.path_params.at("key");
    if (req.path_params.count("value")) value_str = req.path_params.at("value");
    out["key"] = key_str;
    out["value"] = value_str;
    int key;
    if (!parse_int(key_str, key)) {
        out["error"] = "invalid key format";
        out["reason"] = "path parameter 'key' must be an integer";
        json_response(res, 400, out, "invalid_key_format");
        logResponse(res, std::chrono::steady_clock::now() - start);
        return;
    }
    bool inserted = inline_cache.insert_if_absent(key, value_str);
    if (!inserted) {
        out["error"] = "key exists";
        out["existing_value"] = inline_cache.get(key).value_or("");
        out["reason"] = "insert rejected because key already exists";
        json_response(res, 409, out, "conflict_key_exists");
    } else {
        bool persist_ok = true;
        if (persistence_adapter) {
            persist_ok = persistence_adapter->insert(key, value_str);
        }
        if (!persist_ok) {
            inline_cache.erase(key);
            out["error"] = "persistence_failure";
            out["reason"] = "database insert failed";
            json_response(res, 500, out, "persistence_error");
        } else {
            out["created"] = true;
            out["persisted"] = static_cast<bool>(persistence_adapter);
            json_response(res, 201, out, "created");
        }
    }
    logResponse(res, std::chrono::steady_clock::now() - start);
}

void KeyValueServer::bulkUpdateHandler(const httplib::Request& req, httplib::Response& res) {
    auto start = std::chrono::steady_clock::now();
    logRequest(req);

    nlohmann::json out;
    out["endpoint"] = "bulk_update";
    nlohmann::json errors = nlohmann::json::array();

    auto push_error = [&](const std::string& code, const std::string& reason, const nlohmann::json& detail = nlohmann::json()) {
        nlohmann::json err{{"code", code}, {"reason", reason}};
        if (!detail.is_null()) err["detail"] = detail;
        errors.push_back(std::move(err));
    };

    auto finalize = [&](bool success,
                        size_t requested,
                        size_t processed,
                        size_t succeeded,
                        const std::string& mode,
                        const nlohmann::json& results,
                        const std::string& failure_reason) {
        if (!errors.empty()) out["errors"] = errors;
        out["results"] = results;
        nlohmann::json summary;
        summary["requested"] = requested;
        summary["processed"] = processed;
        summary["succeeded"] = succeeded;
        summary["failed"] = processed >= succeeded ? (processed - succeeded) : 0;
        summary["aborted"] = requested >= processed ? (requested - processed) : 0;
        summary["used_transaction"] = (mode == "rollback");
        out["summary"] = summary;
        out["transaction_mode"] = mode;
        out["success"] = success && errors.empty();
        if (!failure_reason.empty()) out["reason"] = failure_reason;
        json_response(res, 200, out, "ok");
        logResponse(res, std::chrono::steady_clock::now() - start);
    };

    if (!persistence_adapter) {
        push_error("persistence_unavailable", "persistence adapter is not configured");
        finalize(false, 0, 0, 0, "not_available", nlohmann::json::array(), "persistence adapter is not configured");
        return;
    }

    if (req.body.empty()) {
        push_error("empty_body", "request body must include JSON with an 'operations' array");
        finalize(false, 0, 0, 0, "not_executed", nlohmann::json::array(), "request body missing");
        return;
    }

    nlohmann::json payload;
    try {
        payload = nlohmann::json::parse(req.body);
    } catch (const std::exception& e) {
        nlohmann::json detail{{"what", e.what()}};
        push_error("parse_error", "failed to parse request JSON", detail);
        finalize(false, 0, 0, 0, "not_executed", nlohmann::json::array(), "failed to parse request JSON");
        return;
    }

    if (!payload.is_object()) {
        push_error("invalid_payload", "request body must be a JSON object");
        finalize(false, 0, 0, 0, "not_executed", nlohmann::json::array(), "request body must be a JSON object");
        return;
    }

    const nlohmann::json* operations_node = nullptr;
    if (payload.contains("operations")) operations_node = &payload["operations"];
    if (!operations_node) {
        push_error("missing_operations", "JSON object must contain an 'operations' array");
        finalize(false, 0, 0, 0, "not_executed", nlohmann::json::array(), "missing operations array");
        return;
    }
    if (!operations_node->is_array()) {
        push_error("invalid_operations_type", "'operations' must be an array of operation objects");
        finalize(false, 0, 0, 0, "not_executed", nlohmann::json::array(), "'operations' must be an array");
        return;
    }

    size_t requested = operations_node->size();

    struct ParsedOperation {
        PersistenceAdapter::Operation op;
        std::string op_name;
        nlohmann::json original;
    };
    std::vector<ParsedOperation> parsed_ops;
    parsed_ops.reserve(requested);

    auto to_lower = [](std::string s) {
        std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
        return s;
    };

    for (size_t idx = 0; idx < operations_node->size(); ++idx) {
        const auto& item = (*operations_node)[idx];
        if (!item.is_object()) {
            push_error("invalid_operation", "each entry in 'operations' must be an object", {{"index", idx}});
            continue;
        }
        if (!item.contains("operation") || !item["operation"].is_string()) {
            push_error("missing_operation_keyword", "operation entry must include string field 'operation'", {{"index", idx}});
            continue;
        }
        std::string op_str = item["operation"].get<std::string>();
        std::string op_lower = to_lower(op_str);

        PersistenceAdapter::OpType op_type;
        if (op_lower == "insert") {
            op_type = PersistenceAdapter::OpType::Insert;
        } else if (op_lower == "update") {
            op_type = PersistenceAdapter::OpType::Update;
        } else if (op_lower == "delete" || op_lower == "remove") {
            op_type = PersistenceAdapter::OpType::Remove;
            op_lower = "delete";
        } else if (op_lower == "get") {
            op_type = PersistenceAdapter::OpType::Get;
        } else {
            push_error("unsupported_operation", "operation must be one of insert, update, delete, get",
                       {{"index", idx}, {"operation", op_str}});
            continue;
        }

        if (!item.contains("key") || !item["key"].is_number_integer()) {
            push_error("invalid_key", "operation must include integer field 'key'", {{"index", idx}});
            continue;
        }
        int key = item["key"].get<int>();

        std::string value;
        bool requires_value = (op_type == PersistenceAdapter::OpType::Insert) || (op_type == PersistenceAdapter::OpType::Update);
        if (requires_value) {
            if (!item.contains("value") || !item["value"].is_string()) {
                push_error("missing_value", "operation requires a string 'value'", {{"index", idx}, {"operation", op_lower}});
                continue;
            }
            value = item["value"].get<std::string>();
        } else if (item.contains("value") && item["value"].is_string()) {
            value = item["value"].get<std::string>();
        }

        parsed_ops.push_back({PersistenceAdapter::Operation{op_type, key, value}, op_lower, item});
    }

    if (!errors.empty()) {
        finalize(false, requested, 0, 0, "not_executed", nlohmann::json::array(), "one or more operations were invalid");
        return;
    }

    if (parsed_ops.empty()) {
        push_error("empty_operations", "'operations' array must include at least one valid operation");
        finalize(false, requested, 0, 0, "not_executed", nlohmann::json::array(), "no valid operations provided");
        return;
    }

    std::vector<PersistenceAdapter::Operation> tx_ops;
    tx_ops.reserve(parsed_ops.size());
    for (const auto& parsed : parsed_ops) {
        tx_ops.push_back(parsed.op);
    }

    size_t processed = 0;
    size_t succeeded = 0;
    bool tx_success = false;
    std::string transaction_mode = "not_executed";
    std::string failure_reason;
    nlohmann::json results = nlohmann::json::array();

#if defined(USE_PG)
    if (auto* adapter = dynamic_cast<PersistenceAdapter*>(persistence_adapter.get())) {
        transaction_mode = "rollback";
        // use async variant to offload DB work to adapter worker pool
        try {
            auto fut = adapter->runTransactionJsonAsync(tx_ops, PersistenceAdapter::TxMode::RollbackOnError);
            auto report = fut.get();
            tx_success = report.value("success", false);

            if (report.contains("results") && report["results"].is_array()) {
                processed = report["results"].size();
                for (size_t i = 0; i < report["results"].size() && i < parsed_ops.size(); ++i) {
                    const auto& item = report["results"][i];
                    nlohmann::json entry;
                    entry["index"] = i;
                    entry["operation"] = parsed_ops[i].op_name;
                    entry["key"] = item.value("key", parsed_ops[i].op.key);
                    entry["status"] = item.value("status", "failed");
                    entry["input"] = parsed_ops[i].original;
                    if (item.contains("value")) entry["value"] = item["value"];
                    if (item.contains("error")) entry["error"] = item["error"];
                    if (entry["status"] == "ok") {
                        succeeded++;
                    } else if (failure_reason.empty() && item.contains("error") && item["error"].is_string()) {
                        failure_reason = item["error"].get<std::string>();
                    }
                    results.push_back(std::move(entry));
                }
            }
        } catch (...) {
            // fall back to synchronous below if async fails
        }
        if (!tx_success && failure_reason.empty()) {
            failure_reason = "transaction rolled back due to failure";
        }
    } else
#endif
    {
        transaction_mode = "rollback";
        PersistenceProvider* provider = persistence_adapter.get();
        std::vector<std::function<void()>> undo;
        undo.reserve(parsed_ops.size());
        tx_success = true;

        for (size_t i = 0; i < parsed_ops.size(); ++i) {
            const auto& parsed = parsed_ops[i];
            nlohmann::json entry;
            entry["index"] = i;
            entry["operation"] = parsed.op_name;
            entry["key"] = parsed.op.key;
            entry["input"] = parsed.original;

            bool ok = true;
            std::string error_msg;

            if (parsed.op.type == PersistenceAdapter::OpType::Insert) {
                auto previous = provider->get(parsed.op.key);
                if (!provider->insert(parsed.op.key, parsed.op.value)) {
                    ok = false;
                    error_msg = "insert failed";
                } else {
                    std::optional<std::string> prev_value;
                    if (previous) prev_value = *previous;
                    undo.push_back([provider, key = parsed.op.key, prev_value]() {
                        if (prev_value) {
                            provider->insert(key, *prev_value);
                        } else {
                            provider->remove(key);
                        }
                    });
                }
            } else if (parsed.op.type == PersistenceAdapter::OpType::Update) {
                auto previous = provider->get(parsed.op.key);
                if (!previous) {
                    ok = false;
                    error_msg = "key not present";
                } else if (!provider->update(parsed.op.key, parsed.op.value)) {
                    ok = false;
                    error_msg = "update failed";
                } else {
                    std::string prev_value = *previous;
                    undo.push_back([provider, key = parsed.op.key, prev_value]() {
                        provider->update(key, prev_value);
                    });
                }
            } else if (parsed.op.type == PersistenceAdapter::OpType::Remove) {
                auto previous = provider->get(parsed.op.key);
                if (!previous) {
                    ok = false;
                    error_msg = "key not present";
                } else if (!provider->remove(parsed.op.key)) {
                    ok = false;
                    error_msg = "delete failed";
                } else {
                    std::string prev_value = *previous;
                    undo.push_back([provider, key = parsed.op.key, prev_value]() {
                        provider->insert(key, prev_value);
                    });
                }
            } else {
                auto value = provider->get(parsed.op.key);
                entry["value"] = value ? nlohmann::json(*value) : nlohmann::json(nullptr);
            }

            entry["status"] = ok ? "ok" : "failed";
            if (!ok) {
                entry["error"] = error_msg;
                failure_reason = error_msg;
                tx_success = false;
                processed = i + 1;
                results.push_back(entry);
                for (auto it = undo.rbegin(); it != undo.rend(); ++it) {
                    (*it)();
                }
                break;
            }

            results.push_back(entry);
            succeeded++;
            processed = i + 1;
        }

        if (tx_success) {
            processed = parsed_ops.size();
        }
    }

    bool has_failed_result = false;
    if (results.is_array()) {
        for (const auto& entry : results) {
            if (!entry.is_object()) continue;
            if (entry.value("status", std::string("failed")) != "ok") {
                has_failed_result = true;
                if (failure_reason.empty() && entry.contains("error") && entry["error"].is_string()) {
                    failure_reason = entry["error"].get<std::string>();
                }
            }
        }
    }

    bool overall_success = tx_success && !has_failed_result && errors.empty();

    if (overall_success) {
        for (size_t i = 0; i < parsed_ops.size(); ++i) {
            const auto& parsed = parsed_ops[i];
            switch (parsed.op.type) {
                case PersistenceAdapter::OpType::Insert:
                case PersistenceAdapter::OpType::Update:
                    inline_cache.update_or_insert(parsed.op.key, parsed.op.value);
                    break;
                case PersistenceAdapter::OpType::Remove:
                    inline_cache.erase(parsed.op.key);
                    break;
                case PersistenceAdapter::OpType::Get: {
                    auto fresh = persistence_adapter->get(parsed.op.key);
                    if (fresh) {
                        inline_cache.update_or_insert(parsed.op.key, *fresh);
                    } else {
                        inline_cache.erase(parsed.op.key);
                    }
                    break;
                }
            }
        }
    }

    finalize(overall_success, requested, processed, succeeded, transaction_mode, results, failure_reason);
}

void KeyValueServer::deletionHandler(const httplib::Request& req, httplib::Response& res) {
    auto start = std::chrono::steady_clock::now();
    logRequest(req);
    std::string key_str;
    nlohmann::json out;
    std::string reason_msg;
    if (!validate_path_params(req, {"key"}, out, reason_msg)) {
        json_response(res, 400, out, reason_msg.c_str());
        logResponse(res, std::chrono::steady_clock::now() - start);
        return;
    }
    if (req.path_params.count("key")) key_str = req.path_params.at("key");
    out["key"] = key_str;
    int key;
    if (!parse_int(key_str, key)) {
        out["error"] = "invalid key format";
        out["reason"] = "path parameter 'key' must be an integer";
        json_response(res, 400, out, "invalid_key_format");
        logResponse(res, std::chrono::steady_clock::now() - start);
        return;
    }
    std::optional<std::string> previous = inline_cache.get(key);
    bool cache_removed = inline_cache.erase(key);
    bool persistence_checked = false;
    bool persistence_removed = false;
    bool persistence_failure = false;

    if (persistence_adapter) {
        persistence_checked = true;
        persistence_removed = persistence_adapter->remove(key);
        if (!persistence_removed && cache_removed) {
            persistence_failure = true;
        }
    }

    if (persistence_failure) {
        if (previous.has_value()) inline_cache.update_or_insert(key, previous.value());
        out["error"] = "persistence_failure";
        out["reason"] = "database delete failed";
        if (persistence_checked) out["persistence_checked"] = true;
        json_response(res, 500, out, "persistence_error");
        logResponse(res, std::chrono::steady_clock::now() - start);
        return;
    }

    if (cache_removed || persistence_removed) {
        if (persistence_checked) out["persistence_checked"] = true;
        json_response(res, 204, out, "deleted");
    } else {
        out["error"] = "not found";
        out["reason"] = persistence_checked ? "key not present in cache or persistence" : "key not present in cache";
        if (persistence_checked) out["persistence_checked"] = true;
        json_response(res, 404, out, "not_found");
    }
    logResponse(res, std::chrono::steady_clock::now() - start);
}

void KeyValueServer::updationHandler(const httplib::Request& req, httplib::Response& res) {
    auto start = std::chrono::steady_clock::now();
    logRequest(req);
    std::string key_str, value_str;
    nlohmann::json out;
    std::string reason_msg;
    if (!validate_path_params(req, {"key","value"}, out, reason_msg)) {
        json_response(res, 400, out, reason_msg.c_str());
        logResponse(res, std::chrono::steady_clock::now() - start);
        return;
    }
    if (req.path_params.count("key")) key_str = req.path_params.at("key");
    if (req.path_params.count("value")) value_str = req.path_params.at("value");
    out["key"] = key_str;
    out["value"] = value_str;
    int key;
    if (!parse_int(key_str, key)) {
        out["error"] = "invalid key format";
        out["reason"] = "path parameter 'key' must be an integer";
        json_response(res, 400, out, "invalid_key_format");
        logResponse(res, std::chrono::steady_clock::now() - start);
        return;
    }
    std::optional<std::string> previous = inline_cache.get(key);
    bool hydrated = false;
    bool persistence_checked = false;
    if (!previous && persistence_adapter) {
        persistence_checked = true;
        if (auto persisted = persistence_adapter->get(key)) {
            inline_cache.update_or_insert(key, *persisted);
            previous = inline_cache.get(key);
            hydrated = true;
        }
    }

    if (!previous) {
        out["error"] = "not found";
        out["reason"] = persistence_checked ? "key not present in cache or persistence" : "key not present in cache";
        if (persistence_checked) out["persistence_checked"] = true;
        json_response(res, 404, out, "not_found");
        logResponse(res, std::chrono::steady_clock::now() - start);
        return;
    }

    bool cache_updated = inline_cache.update(key, value_str);
    if (!cache_updated) {
        out["error"] = "not found";
        out["reason"] = "key not present in cache";
        if (persistence_checked) out["persistence_checked"] = true;
        json_response(res, 404, out, "not_found");
        logResponse(res, std::chrono::steady_clock::now() - start);
        return;
    }

    bool persist_ok = true;
    if (persistence_adapter) {
        persistence_checked = true;
    persist_ok = persistence_adapter->update(key, value_str);
    }

    if (!persist_ok) {
        if (previous.has_value()) inline_cache.update(key, previous.value());
        out["error"] = "persistence_failure";
        out["reason"] = "database update failed";
        if (persistence_checked) out["persistence_checked"] = true;
        json_response(res, 500, out, "persistence_error");
    } else {
        out["updated"] = true;
        if (persistence_adapter) out["persisted"] = true;
        if (hydrated) out["hydrated_from_persistence"] = true;
        if (persistence_checked) out["persistence_checked"] = true;
        json_response(res, 200, out, "updated");
    }
    logResponse(res, std::chrono::steady_clock::now() - start);
}

void KeyValueServer::setupRoutes() {
    server_.Get("/", [this](const auto& r, auto& s) { indexHandler(r, s); });
    server_.Get("/home", [this](const auto& r, auto& s) { homeHandler(r, s); });
    server_.Get("/get_key/:key_id", [this](const auto& r, auto& s) { getKeyHandler(r, s); });
    server_.Patch("/bulk_query", [this](const auto& r, auto& s) { bulkQueryHandler(r, s); });
    server_.Post("/insert/:key/:value", [this](const auto& r, auto& s) { insertionHandler(r, s); });
    server_.Post("/bulk_update", [this](const auto& r, auto& s) { bulkUpdateHandler(r, s); });
    server_.Delete("/delete_key/:key", [this](const auto& r, auto& s) { deletionHandler(r, s); });
    server_.Put("/update_key/:key/:value", [this](const auto& r, auto& s) { updationHandler(r, s); });
    server_.Get("/health", [this](const auto& r, auto& s) { healthHandler(r, s); });
    server_.Get("/metrics", [this](const auto& r, auto& s) { metricsHandler(r, s); });
    server_.Get("/stop", [this](const auto& r, auto& s) { stopHandler(r, s); });
}

bool KeyValueServer::start() {
    auto emit_startup_log = [&](bool success, const std::string& message) {
            if (!logging_enabled) return;
            if (json_logging_enabled) {
            auto now_sys = std::chrono::system_clock::now();
            auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now_sys.time_since_epoch()).count();
            nlohmann::json j;
            j["type"] = "startup";
            j["start_time_ms"] = ms;
            switch (inline_cache.policy()) {
                case InlineCache::Policy::LRU: j["cache_policy"] = "LRU"; break;
                case InlineCache::Policy::FIFO: j["cache_policy"] = "FIFO"; break;
                default: j["cache_policy"] = "Random"; break;
            }
            j["db_connection_status"] = db_connection_status;
            j["json_logging_enabled"] = json_logging_enabled;
            j["listen"] = { {"host", host_}, {"port", port_} };
            j["ready"] = success;
            if (!message.empty()) {
                if (success) j["message"] = message;
                else j["error"] = message;
            }
            if (!success) j["action"] = "shutdown";
            std::cout << j.dump() << std::endl;
            } else {
                if (success) {
                    std::cout << "Http server listening at " << host_ << ":" << port_
                              << " policy=" << (inline_cache.policy() == InlineCache::Policy::LRU ? "LRU" : inline_cache.policy() == InlineCache::Policy::FIFO ? "FIFO" : "Random")
                              << " db_status=" << db_connection_status
                              << " json_logs=" << (json_logging_enabled?"1":"0");
                    if (!message.empty()) {
                        std::cout << ' ' << message;
                    }
                    std::cout << "\n";
                } else {
                    std::cerr << "Startup aborted: " << message
                              << " (db_status=" << db_connection_status << ")\n";
                }
            }
    };

    auto abort_startup = [&](const std::string& status, const std::string& reason) {
        db_connection_status = status;
        emit_startup_log(false, reason);
        return false;
    };

    /* uncomment this for compiling test file, also in prod environment make sure USE_PG flag is set true in build*/
    // Attempt to initialize persistence adapter. Startup is aborted if persistence is unavailable.
    // if (!persistence_adapter) {
// #if defined(USE_PG)
        try {
            std::string conn = load_conninfo();
            persistence_adapter = std::make_unique<PersistenceAdapter>(conn);
            db_connection_status = "ok";
            persistence_injected = false;
        } catch (const std::exception &e) {
            return abort_startup(std::string("failed: ") + e.what(), std::string("unable to connect to persistence backend: ") + e.what());
        }
// #else
//         return abort_startup("unavailable", "persistence adapter required but binary built without USE_PG support");
// #endif
//     } else if (persistence_injected && db_connection_status.empty()) {
//         db_connection_status = "injected";
//     }

    // if (!persistence_adapter) {
    //     return abort_startup("uninitialized", "persistence adapter could not be initialized");
    // }

    // Preload cache from persistence for keys 1..1000 before accepting connections (unless disabled).
    size_t preload_attempts = 0;
    size_t preload_loaded = 0;
    if (!skip_preload && persistence_adapter) {
        // iterate and try to load; do not abort startup on missing keys — only log progress
        const int preload_start = 1;
        const int preload_end = 1000;
        for (int k = preload_start; k <= preload_end; ++k) {
            try {
                auto vptr = persistence_adapter->get(k);
                ++preload_attempts;
                if (vptr && !vptr->empty()) {
                    // insert into inline cache (insert_if_absent keeps existing entries)
                    inline_cache.insert_if_absent(k, *vptr);
                    ++preload_loaded;
                }
                if (preload_attempts % 100 == 0) {
                    if (logging_enabled) {
                        std::cout << "Preload progress: attempted=" << preload_attempts << " loaded=" << preload_loaded << "\n";
                    }
                }
            } catch (const std::exception &e) {
                // log and continue
                if (logging_enabled) std::cerr << "Preload error for key=" << k << " : " << e.what() << "\n";
            }
        }
    }

    // emit startup log with preload summary
    std::ostringstream startup_message;
    startup_message << "preload_attempts=" << preload_attempts << " preload_loaded=" << preload_loaded;
    emit_startup_log(true, startup_message.str());
    return server_.listen(host_, port_);
}

void KeyValueServer::stop() { server_.stop(); }

httplib::Server& KeyValueServer::raw() { return server_; }

void KeyValueServer::setPersistenceProvider(std::unique_ptr<PersistenceProvider> provider, const std::string& status_label) {
    persistence_adapter = std::move(provider);
    persistence_injected = persistence_adapter != nullptr;
    if (persistence_injected) {
        db_connection_status = status_label;
    } else if (db_connection_status.empty()) {
        db_connection_status = "not configured";
    }
}

// ---- Helpers ----
void KeyValueServer::json_response(httplib::Response& res, int status, const nlohmann::json& j, const char* reason) {
    res.status = status;
    if (status == 204) {
        res.set_content("", "application/json");
    } else {
        res.set_content(j.dump(), "application/json");
    }
    if (reason) {
        res.reason = reason;
    }
}

bool KeyValueServer::parse_int(const std::string& s, int& out) {
    try {
        size_t idx=0; int v = std::stoi(s, &idx); if (idx != s.size()) return false; out = v; return true;
    } catch (...) { return false; }
}

void KeyValueServer::healthHandler(const httplib::Request& req, httplib::Response& res) {
    auto start = std::chrono::steady_clock::now();
    logRequest(req);
    auto now = std::chrono::steady_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now - server_boot_time).count();
    nlohmann::json out{{"status","ok"},{"uptime_ms",ms}};
    json_response(res, 200, out, "ok");
    logResponse(res, std::chrono::steady_clock::now() - start);
}

void KeyValueServer::metricsHandler(const httplib::Request& req, httplib::Response& res) {
    auto start = std::chrono::steady_clock::now();
    logRequest(req);
    if (!metrics_enabled) {
        // Lightweight response when metrics are disabled: avoid any /proc or /sys reads.
        nlohmann::json out{{"metrics","disabled"},{"reason","metrics collection disabled by server configuration"}};
        json_response(res, 200, out, "ok");
        logResponse(res, std::chrono::steady_clock::now() - start);
        return;
    }
    auto st = inline_cache.stats();
    nlohmann::json out{{"entries",st.size_entries},{"bytes",st.bytes_estimated},{"hits",st.hits},{"misses",st.misses},{"evictions",st.evictions}};
    // attach persistence adapter pool metrics if available
    if (auto* ada = dynamic_cast<PersistenceAdapter*>(persistence_adapter.get())) {
        try {
            out["persistence_pool"] = ada->poolMetrics();
        } catch (...) {}
    }

    // System metrics (Linux-specific: /proc and /sys). We compute deltas since the last sample
    try {
        static std::mutex sys_metrics_mutex;
        struct CpuSample { unsigned long long user=0, nice=0, system=0, idle=0, iowait=0, irq=0, softirq=0, steal=0; };
        struct SysSnapshot {
            CpuSample cpu;
            unsigned long long disk_sectors_read = 0;
            unsigned long long disk_sectors_written = 0;
            unsigned long long disk_read_ios = 0;
            unsigned long long disk_write_ios = 0;
            unsigned long long disk_io_ms = 0; // aggregated time doing I/Os (ms)
            unsigned long long net_rx_bytes = 0;
            unsigned long long net_tx_bytes = 0;
            std::chrono::steady_clock::time_point ts = std::chrono::steady_clock::now();
            size_t disk_device_count = 0;
        };
        static SysSnapshot last_snapshot;

        auto read_cpu = [&]() -> CpuSample {
            CpuSample s;
            std::ifstream f("/proc/stat");
            if (!f.is_open()) return s;
            std::string line;
            while (std::getline(f, line)) {
                if (line.rfind("cpu ", 0) == 0) {
                    std::istringstream ss(line);
                    std::string cpu_label;
                    ss >> cpu_label;
                    ss >> s.user >> s.nice >> s.system >> s.idle >> s.iowait >> s.irq >> s.softirq >> s.steal;
                    break;
                }
            }
            return s;
        };

        auto now = std::chrono::steady_clock::now();
        SysSnapshot cur;
        cur.ts = now;
        cur.cpu = read_cpu();

        // Memory
        std::ifstream memf("/proc/meminfo");
        if (memf.is_open()) {
            std::string l;
            unsigned long long mem_total_kb = 0, mem_free_kb = 0, mem_available_kb = 0;
            while (std::getline(memf, l)) {
                if (l.rfind("MemTotal:", 0) == 0) {
                    std::istringstream ss(l.substr(9)); ss >> mem_total_kb; // kB
                } else if (l.rfind("MemFree:", 0) == 0) {
                    std::istringstream ss(l.substr(8)); ss >> mem_free_kb;
                } else if (l.rfind("MemAvailable:", 0) == 0) {
                    std::istringstream ss(l.substr(13)); ss >> mem_available_kb;
                }
            }
            out["memory_kb"] = { {"total", mem_total_kb}, {"free", mem_free_kb}, {"available", mem_available_kb} };
        }

        // Disk I/O: aggregate /sys/block/*/stat and collect sectors and io_ms (field 10 per-device)
        unsigned long long total_reads_sectors = 0, total_writes_sectors = 0, total_read_ios = 0, total_write_ios = 0, total_io_ms = 0;
        size_t device_count = 0;
        for (const auto &entry : std::filesystem::directory_iterator("/sys/block")) {
            try {
                std::string statpath = entry.path().string() + "/stat";
                std::ifstream sf(statpath);
                if (!sf.is_open()) continue;
                std::string ln;
                if (!std::getline(sf, ln)) continue;
                std::istringstream ss(ln);
                std::vector<unsigned long long> fields;
                unsigned long long v;
                while (ss >> v) fields.push_back(v);
                if (fields.size() >= 11) {
                    // fields[2] = sectors read, fields[6] = sectors written, fields[0]=reads completed, fields[4]=writes completed
                    total_reads_sectors += fields[2];
                    total_writes_sectors += fields[6];
                    total_read_ios += fields[0];
                    total_write_ios += fields[4];
                    // field 9 (0-based) is time spent doing I/Os (ms)
                    total_io_ms += fields[9];
                    device_count++;
                } else if (fields.size() >= 7) {
                    total_reads_sectors += (fields.size() > 2 ? fields[2] : 0);
                    total_writes_sectors += (fields.size() > 6 ? fields[6] : 0);
                    total_read_ios += (fields.size() > 0 ? fields[0] : 0);
                    total_write_ios += (fields.size() > 4 ? fields[4] : 0);
                    // no io_ms available
                    device_count++;
                }
            } catch (...) {
                continue;
            }
        }
        cur.disk_sectors_read = total_reads_sectors;
        cur.disk_sectors_written = total_writes_sectors;
        cur.disk_read_ios = total_read_ios;
        cur.disk_write_ios = total_write_ios;
        cur.disk_io_ms = total_io_ms;
        cur.disk_device_count = device_count;

        // Network
        std::ifstream netf("/proc/net/dev");
        if (netf.is_open()) {
            std::string line;
            std::getline(netf, line);
            std::getline(netf, line);
            unsigned long long total_rx = 0, total_tx = 0;
            while (std::getline(netf, line)) {
                std::istringstream ss(line);
                std::string iface;
                if (!(ss >> iface)) continue;
                if (iface.back() == ':') iface.pop_back();
                unsigned long long rx_bytes=0, rx_packets, rx_errs, rx_drop, rx_fifo, rx_frame, rx_compressed, rx_multicast;
                unsigned long long tx_bytes=0, tx_packets, tx_errs, tx_drop, tx_fifo, tx_colls, tx_carrier, tx_compressed;
                ss >> rx_bytes >> rx_packets >> rx_errs >> rx_drop >> rx_fifo >> rx_frame >> rx_compressed >> rx_multicast
                   >> tx_bytes >> tx_packets >> tx_errs >> tx_drop >> tx_fifo >> tx_colls >> tx_carrier >> tx_compressed;
                if (iface == "lo") continue;
                total_rx += rx_bytes;
                total_tx += tx_bytes;
            }
            cur.net_rx_bytes = total_rx;
            cur.net_tx_bytes = total_tx;
        }

        // Compute deltas and rates
        double cpu_util = 0.0;
        double elapsed_s = 0.0;
        unsigned long long delta_read_bytes = 0, delta_write_bytes = 0, delta_read_ios = 0, delta_write_ios = 0;
        unsigned long long delta_rx = 0, delta_tx = 0;
        double disk_util_pct = 0.0;

        {
            std::lock_guard<std::mutex> lk(sys_metrics_mutex);
            // CPU
            unsigned long long prev_idle = last_snapshot.cpu.idle + last_snapshot.cpu.iowait;
            unsigned long long idle = cur.cpu.idle + cur.cpu.iowait;
            unsigned long long prev_non_idle = last_snapshot.cpu.user + last_snapshot.cpu.nice + last_snapshot.cpu.system + last_snapshot.cpu.irq + last_snapshot.cpu.softirq + last_snapshot.cpu.steal;
            unsigned long long non_idle = cur.cpu.user + cur.cpu.nice + cur.cpu.system + cur.cpu.irq + cur.cpu.softirq + cur.cpu.steal;
            unsigned long long prev_total = prev_idle + prev_non_idle;
            unsigned long long total = idle + non_idle;
            unsigned long long totald = 0, idled = 0;
            if (total >= prev_total) totald = total - prev_total; else totald = 0;
            if (idle >= prev_idle) idled = idle - prev_idle; else idled = 0;
            if (totald > 0) cpu_util = (double)(totald - idled) * 100.0 / (double)totald;

            // time delta
            auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(cur.ts - last_snapshot.ts).count();
            if (ms > 0) elapsed_s = (double)ms / 1000.0;

            // disk deltas
            if (cur.disk_sectors_read >= last_snapshot.disk_sectors_read) delta_read_bytes = (cur.disk_sectors_read - last_snapshot.disk_sectors_read) * 512ULL;
            if (cur.disk_sectors_written >= last_snapshot.disk_sectors_written) delta_write_bytes = (cur.disk_sectors_written - last_snapshot.disk_sectors_written) * 512ULL;
            if (cur.disk_read_ios >= last_snapshot.disk_read_ios) delta_read_ios = cur.disk_read_ios - last_snapshot.disk_read_ios;
            if (cur.disk_write_ios >= last_snapshot.disk_write_ios) delta_write_ios = cur.disk_write_ios - last_snapshot.disk_write_ios;

            // disk utilization: use aggregated io_ms (field 10) per device
            unsigned long long delta_io_ms = 0;
            if (cur.disk_io_ms >= last_snapshot.disk_io_ms) delta_io_ms = cur.disk_io_ms - last_snapshot.disk_io_ms;
            if (elapsed_s > 0.0 && cur.disk_device_count > 0) {
                double elapsed_ms = elapsed_s * 1000.0;
                // average busy percent across devices (previous behavior)
                double avg_per_device = (double)delta_io_ms / (elapsed_ms * (double)cur.disk_device_count) * 100.0;
                if (avg_per_device < 0.0) avg_per_device = 0.0;
                if (avg_per_device > 100.0) avg_per_device = 100.0;
                disk_util_pct = avg_per_device;
                // aggregate busy percent: total device-ms per wall-ms (not divided by device count)
                double aggregate_pct = 0.0;
                aggregate_pct = (double)delta_io_ms / elapsed_ms * 100.0;
                if (aggregate_pct < 0.0) aggregate_pct = 0.0;
                // note: aggregate_pct can exceed 100% if there are multiple devices (it represents total device-ms per wall-ms)
                out["disk_utilization_percent_aggregate"] = aggregate_pct;
            }

            // network deltas
            if (cur.net_rx_bytes >= last_snapshot.net_rx_bytes) delta_rx = cur.net_rx_bytes - last_snapshot.net_rx_bytes;
            if (cur.net_tx_bytes >= last_snapshot.net_tx_bytes) delta_tx = cur.net_tx_bytes - last_snapshot.net_tx_bytes;

            // update last_snapshot
            last_snapshot = cur;
        }

        out["cpu_utilization_percent"] = cpu_util;
        out["disk_read_bytes"] = cur.disk_sectors_read * 512ULL;
        out["disk_write_bytes"] = cur.disk_sectors_written * 512ULL;
        out["disk_io_ops"] = { {"read_ios", cur.disk_read_ios}, {"write_ios", cur.disk_write_ios} };
    out["disk_utilization_percent_avg_per_device"] = disk_util_pct;

        // per-second rates (if elapsed_s > 0)
        if (elapsed_s > 0.0) {
            out["disk_read_bytes_per_sec"] = (double)delta_read_bytes / elapsed_s;
            out["disk_write_bytes_per_sec"] = (double)delta_write_bytes / elapsed_s;
            out["disk_read_ios_per_sec"] = (double)delta_read_ios / elapsed_s;
            out["disk_write_ios_per_sec"] = (double)delta_write_ios / elapsed_s;
            out["network_bytes"] = { {"rx", cur.net_rx_bytes}, {"tx", cur.net_tx_bytes} };
            out["network_rx_bytes_per_sec"] = (double)delta_rx / elapsed_s;
            out["network_tx_bytes_per_sec"] = (double)delta_tx / elapsed_s;
        } else {
            out["disk_read_bytes_per_sec"] = 0.0;
            out["disk_write_bytes_per_sec"] = 0.0;
            out["disk_read_ios_per_sec"] = 0.0;
            out["disk_write_ios_per_sec"] = 0.0;
            out["network_bytes"] = { {"rx", cur.net_rx_bytes}, {"tx", cur.net_tx_bytes} };
            out["network_rx_bytes_per_sec"] = 0.0;
            out["network_tx_bytes_per_sec"] = 0.0;
        }
        out["disk_devices_reported"] = (int)cur.disk_device_count;

        // Process-level metrics (Linux /proc/self)
        try {
            // RSS and VMS from /proc/self/statm (in pages)
            std::ifstream statm("/proc/self/statm");
            if (statm.is_open()) {
                unsigned long size_pages = 0, resident_pages = 0;
                statm >> size_pages >> resident_pages;
                long page_size = sysconf(_SC_PAGESIZE);
                unsigned long vms_kb = 0, rss_kb = 0;
                if (page_size > 0) {
                    vms_kb = (size_pages * (unsigned long)page_size) / 1024UL;
                    rss_kb = (resident_pages * (unsigned long)page_size) / 1024UL;
                }
                out["process"] = {
                    {"vms_kb", vms_kb},
                    {"rss_kb", rss_kb}
                };
            }

            // Thread count and other info from /proc/self/status
            std::ifstream statusf("/proc/self/status");
            if (statusf.is_open()) {
                std::string line;
                int threads = 0;
                while (std::getline(statusf, line)) {
                    if (line.rfind("Threads:", 0) == 0) {
                        std::istringstream ss(line.substr(8)); ss >> threads;
                        break;
                    }
                }
                if (out.contains("process") && out["process"].is_object()) {
                    out["process"]["threads"] = threads;
                } else {
                    out["process"] = { {"threads", threads} };
                }
            }

            // Open file descriptor count via /proc/self/fd
            size_t fd_count = 0;
            for (const auto &entry : std::filesystem::directory_iterator("/proc/self/fd")) {
                (void)entry;
                ++fd_count;
            }
            if (out.contains("process") && out["process"].is_object()) {
                out["process"]["open_fds"] = (int)fd_count;
            } else {
                out["process"] = { {"open_fds", (int)fd_count} };
            }
        } catch (...) {
            // ignore process metric failures
        }
    } catch (...) {
        // don't let system metrics break the endpoint
    }
    json_response(res, 200, out, "ok");
    logResponse(res, std::chrono::steady_clock::now() - start);
}

void KeyValueServer::stopHandler(const httplib::Request& req, httplib::Response& res) {
    auto start = std::chrono::steady_clock::now();
    logRequest(req);
    nlohmann::json out{{"stopping",true}};
    json_response(res, 200, out, "ok");
    logResponse(res, std::chrono::steady_clock::now() - start);
    stop();
}
