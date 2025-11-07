// Refactored implementation: KeyValueServer class defined in server.h
#include "server.h"
#include <iostream>
#include <chrono>
#include <sstream>
#include <fstream>
#include <optional>
#include <utility>

const std::vector<KeyValueServer::RouteDescriptor> KeyValueServer::routes_json = {
    {"GET", "/", "Machine-readable service catalog"},
    {"GET", "/home", "Formatted documentation for available routes"},
    {"GET", "/get_key/:key_id", "Return the cached value for the provided numeric key"},
    {"POST", "/bulk_query", "Retrieve multiple keys in one request; missing keys noted in response"},
    {"POST", "/insert/:key/:value", "Insert a key/value pair; conflicts return 409 with existing value"},
    {"PATCH", "/bulk_update", "Partial commit pipeline for insert/update operations"},
    {"DELETE", "/delete_key/:key", "Remove the provided key from the cache"},
    {"PUT", "/update_key/:key/:value", "Update an existing key with a new value"},
    {"GET", "/health", "Report service health and uptime"},
    {"GET", "/metrics", "Expose cache metrics including hit/miss counts"},
    {"GET", "/stop", "Gracefully stop the server (testing/debug only)"}
};

KeyValueServer::KeyValueServer(const std::string& host, int port, InlineCache::Policy policy, bool jsonLogging)
    : host_(host), port_(port), inline_cache(policy), json_logging_enabled(jsonLogging) {
    server_boot_time = std::chrono::steady_clock::now();
}

KeyValueServer::~KeyValueServer() = default;

void KeyValueServer::logRequest(const httplib::Request& req) {
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
    std::string keyStr, valStr;
    nlohmann::json out;
    std::string reason_msg;
    if (!validate_path_params(req, {"key","value"}, out, reason_msg)) {
        json_response(res, 400, out, reason_msg.c_str());
        logResponse(res, std::chrono::steady_clock::now() - start);
        return;
    }
    if (req.path_params.count("key")) keyStr = req.path_params.at("key");
    if (req.path_params.count("value")) valStr = req.path_params.at("value");
    out["key"] = keyStr;
    out["value"] = valStr;
    int key;
    if (!parse_int(keyStr, key)) {
        out["error"] = "invalid key format";
        out["reason"] = "path parameter 'key' must be an integer";
        json_response(res, 400, out, "invalid_key_format");
        logResponse(res, std::chrono::steady_clock::now() - start);
        return;
    }
    bool inserted = inline_cache.insert_if_absent(key, valStr);
    if (!inserted) {
        out["error"] = "key exists";
        out["existing_value"] = inline_cache.get(key).value_or("");
        out["reason"] = "insert rejected because key already exists";
        json_response(res, 409, out, "conflict_key_exists");
    } else {
        bool persist_ok = true;
        if (persistence_adapter) {
            persist_ok = persistence_adapter->insert(key, valStr);
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
    if (!req.body.empty()) {
        try {
            auto j = nlohmann::json::parse(req.body);
            if (!j.is_array()) {
                out["error"] = "expected array of {op,key,value}";
                out["reason"] = "request body must be a JSON array of operation objects";
                json_response(res, 400, out, "invalid_body");
            } else {
                nlohmann::json results = nlohmann::json::array();
                for (auto& op : j) {
                    nlohmann::json r;
                    if (!(op.contains("key") && op["key"].is_number_integer())) {
                        r["error"] = "invalid key";
                        r["reason"] = "operation key must be an integer";
                        r["success"] = false;
                        results.push_back(r);
                        continue;
                    }

                    int k = op["key"].get<int>();
                    std::string v = op.value("value", "");
                    std::string action = op.value("op", "update_or_insert");
                    r["key"] = k;
                    r["op"] = action;
                    r["value"] = v;

                    bool success = false;
                    bool persistence_checked = false;

                    if (action == "insert") {
                        bool inserted = inline_cache.insert_if_absent(k, v);
                        r["inserted_new"] = inserted;
                        if (!inserted) {
                            r["error"] = "exists";
                            r["reason"] = "insert failed because key already present in cache";
                        } else {
                            bool persist_ok = true;
                            if (persistence_adapter) {
                                persistence_checked = true;
                                persist_ok = persistence_adapter->insert(k, v);
                            }
                            if (!persist_ok) {
                                inline_cache.erase(k);
                                r["error"] = "persistence_failure";
                                r["reason"] = "database insert failed";
                            } else {
                                success = true;
                                if (persistence_adapter) r["persisted"] = true;
                            }
                        }
                    } else if (action == "update") {
                        std::optional<std::string> previous = inline_cache.get(k);
                        bool hydrated = false;
                        if (!previous && persistence_adapter) {
                            persistence_checked = true;
                            if (auto persisted = persistence_adapter->get(k)) {
                                inline_cache.update_or_insert(k, *persisted);
                                previous = inline_cache.get(k);
                                hydrated = true;
                            }
                        }
                        if (!previous) {
                            r["error"] = "missing";
                            r["reason"] = "update failed because key not present";
                        } else {
                            bool cache_updated = inline_cache.update(k, v);
                            if (!cache_updated) {
                                r["error"] = "missing";
                                r["reason"] = "update failed because key not present";
                            } else {
                                bool persist_ok = true;
                                if (persistence_adapter) {
                                    persistence_checked = true;
                                    persist_ok = persistence_adapter->update(k, v);
                                }
                                if (!persist_ok) {
                                    if (previous.has_value()) inline_cache.update_or_insert(k, previous.value());
                                    else inline_cache.erase(k);
                                    r["error"] = "persistence_failure";
                                    r["reason"] = "database update failed";
                                } else {
                                    success = true;
                                    if (hydrated) r["hydrated_from_persistence"] = true;
                                }
                            }
                        }
                    } else if (action == "update_or_insert") {
                        std::optional<std::string> previous = inline_cache.get(k);
                        bool inserted = inline_cache.update_or_insert(k, v);
                        r["inserted_new"] = inserted;
                        bool persist_ok = true;
                        if (persistence_adapter) {
                            persistence_checked = true;
                            persist_ok = persistence_adapter->insert(k, v);
                        }
                        if (!persist_ok) {
                            if (previous.has_value()) inline_cache.update_or_insert(k, previous.value());
                            else inline_cache.erase(k);
                            r["error"] = "persistence_failure";
                            r["reason"] = "database upsert failed";
                        } else {
                            success = true;
                            if (persistence_adapter) r["persisted"] = true;
                        }
                    } else {
                        r["error"] = "invalid op";
                        r["reason"] = "operation must be one of insert, update, update_or_insert";
                    }

                    if (persistence_checked) r["persistence_checked"] = true;
                    r["success"] = success;
                    results.push_back(r);
                }
                out["results"] = results;
                json_response(res, 200, out, "ok");
            }
        } catch (const std::exception& e) {
            out["error"] = std::string("parse error: ") + e.what();
            out["reason"] = "failed to parse request JSON";
            json_response(res, 400, out, "parse_error");
        }
    } else {
        out["info"] = "empty body";
        out["reason"] = "no payload provided";
        json_response(res, 200, out, "ok");
    }
    logResponse(res, std::chrono::steady_clock::now() - start);
}

void KeyValueServer::deletionHandler(const httplib::Request& req, httplib::Response& res) {
    auto start = std::chrono::steady_clock::now();
    logRequest(req);
    std::string keyStr;
    nlohmann::json out;
    std::string reason_msg;
    if (!validate_path_params(req, {"key"}, out, reason_msg)) {
        json_response(res, 400, out, reason_msg.c_str());
        logResponse(res, std::chrono::steady_clock::now() - start);
        return;
    }
    if (req.path_params.count("key")) keyStr = req.path_params.at("key");
    out["key"] = keyStr;
    int key;
    if (!parse_int(keyStr, key)) {
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
    std::string keyStr, valStr;
    nlohmann::json out;
    std::string reason_msg;
    if (!validate_path_params(req, {"key","value"}, out, reason_msg)) {
        json_response(res, 400, out, reason_msg.c_str());
        logResponse(res, std::chrono::steady_clock::now() - start);
        return;
    }
    if (req.path_params.count("key")) keyStr = req.path_params.at("key");
    if (req.path_params.count("value")) valStr = req.path_params.at("value");
    out["key"] = keyStr;
    out["value"] = valStr;
    int key;
    if (!parse_int(keyStr, key)) {
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

    bool cache_updated = inline_cache.update(key, valStr);
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
        persist_ok = persistence_adapter->update(key, valStr);
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
    server_.Post("/bulk_query", [this](const auto& r, auto& s) { bulkQueryHandler(r, s); });
    server_.Post("/insert/:key/:value", [this](const auto& r, auto& s) { insertionHandler(r, s); });
    server_.Patch("/bulk_update", [this](const auto& r, auto& s) { bulkUpdateHandler(r, s); });
    server_.Delete("/delete_key/:key", [this](const auto& r, auto& s) { deletionHandler(r, s); });
    server_.Put("/update_key/:key/:value", [this](const auto& r, auto& s) { updationHandler(r, s); });
    server_.Get("/health", [this](const auto& r, auto& s) { healthHandler(r, s); });
    server_.Get("/metrics", [this](const auto& r, auto& s) { metricsHandler(r, s); });
    server_.Get("/stop", [this](const auto& r, auto& s) { stopHandler(r, s); });
}

bool KeyValueServer::start() {
    auto emit_startup_log = [&](bool success, const std::string& message) {
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

    emit_startup_log(true, "");
    return server_.listen(host_, port_);
}

void KeyValueServer::stop() { server_.stop(); }

httplib::Server& KeyValueServer::raw() { return server_; }

void KeyValueServer::setPersistenceProvider(std::unique_ptr<PersistenceProvider> provider, const std::string& statusLabel) {
    persistence_adapter = std::move(provider);
    persistence_injected = persistence_adapter != nullptr;
    if (persistence_injected) {
        db_connection_status = statusLabel;
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
    auto st = inline_cache.stats();
    nlohmann::json out{{"entries",st.size_entries},{"bytes",st.bytes_estimated},{"hits",st.hits},{"misses",st.misses},{"evictions",st.evictions}};
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
