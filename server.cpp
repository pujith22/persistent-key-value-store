// Refactored implementation: KeyValueServer class defined in server.h
#include "server.h"
#include <iostream>
#include <chrono>

// Static home page HTML content
const std::string KeyValueServer::kHomePageHtml =
    "<html> <h1 style= \"color: brown;\"> Welcome to Persistent Key Value Server!!</h1>"
    "<h3 style= \"color: green;\"> Author: Pujith Sai Kumar Korepara </h3>"
    "<h3 style= \"color: green;\"> ID: 25M0787 </h3>"
    "<table> <tr style= \"color: #191970;\"> <th> Route </th> <th> Description </th> </tr>"
    "<tr style= \"color: blue; font: 20px; font-style: italic;\"> <td> GET /get_key/:key_id  </td>  <td> Returns value associated with key if present else will throws an error response. </td> </tr>"
    "<tr style= \"color: blue; font: 20px; font-style: italic;\"> <td> POST /bulk_query </td> <td> Tries to retrieve values for all queries in this post request, ignoring keys that are not present and passing the corresponding information in the response. </td> </tr>"
    "<tr style= \"color: blue; font: 20px; font-style: italic;\"> <td> POST /insert/:key/:value </td> <td> Inserts key-value pair provided into the database if it doesn't exit already, otherwise throws an error response. </td> </tr>"
    "<tr style= \"color: blue; font: 20px; font-style: italic;\"> <td> PATCH /bulk_update </td> <td>Process all insertions and update queries ignoring errors and sending response accordingly (partial commit). </td> </tr>"
    "<tr style= \"color: blue; font: 20px; font-style: italic;\"> <td> DELETE /delete_key/:key </td> <td> Deletes key from the key-value store if it exists, otherwise throws an error response. </td> </tr>"
    "<tr style= \"color: blue; font: 20px; font-style: italic;\"> <td> PUT /update_key/:key/:value </td> <td> Updates value corresponding to 'key' with 'value' if 'key' exists; error otherwise. </td> </tr>"
    "<tr style= \"color: blue; font: 20px; font-style: italic;\"> <td> GET /health </td> <td> Server Health params like uptime etc. </td> </tr>"
    "<tr style= \"color: blue; font: 20px; font-style: italic;\"> <td> GET /metrics </td> <td> Get cache metrics like hitrate/missrate entries and evictions </td> </tr>"
    "</table> </html>";

KeyValueServer::KeyValueServer(const std::string& host, int port)
    : host_(host), port_(port), inline_cache(InlineCache::Policy::LRU) {
    server_boot_time = std::chrono::steady_clock::now();
}

KeyValueServer::KeyValueServer(const std::string& host, int port, InlineCache::Policy policy)
    : host_(host), port_(port), inline_cache(policy) {
    server_boot_time = std::chrono::steady_clock::now();
}

KeyValueServer::KeyValueServer(const std::string& host, int port, InlineCache::Policy policy, bool jsonLogging)
    : host_(host), port_(port), inline_cache(policy), json_logging_enabled(jsonLogging) {
    server_boot_time = std::chrono::steady_clock::now();
}

KeyValueServer::~KeyValueServer() = default;

void KeyValueServer::logRequest(const httplib::Request& req) {
    if (json_logging_enabled) {
        nlohmann::json j{{"type","request"},{"method",req.method},{"path",req.path}};
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
        std::cout << j.dump() << std::endl;
    } else {
        std::cout << "[RESPONSE] status=" << res.status << " reason=" << res.reason
                  << " ct=" << res.get_header_value("Content-Type")
                  << " bytes=" << res.body.size()
                  << " duration_ms=" << ms << "\n";
    }
}

void KeyValueServer::homeHandler(const httplib::Request& req, httplib::Response& res) {
    auto start = std::chrono::steady_clock::now();
    logRequest(req);
    res.status = 200;
    res.set_content(kHomePageHtml, "text/html");
    logResponse(res, std::chrono::steady_clock::now() - start);
}

void KeyValueServer::getKeyHandler(const httplib::Request& req, httplib::Response& res) {
    auto start = std::chrono::steady_clock::now();
    logRequest(req);
    std::string id;
    if (req.has_param("key_id")) id = req.get_param_value("key_id");
    else if (req.path_params.count("key_id")) id = req.path_params.at("key_id");
    int key;
    nlohmann::json out;
    out["query_key"] = id;
    if (!parse_int(id, key)) {
        out["error"] = "invalid key format";
        json_response(res, 400, out);
        logResponse(res, std::chrono::steady_clock::now() - start);
        return;
    }
    auto v = inline_cache.get(key);
    if (v) {
        out["found"] = true;
        out["value"] = *v;
        json_response(res, 200, out);
    } else {
        out["found"] = false;
        json_response(res, 404, out);
    }
    logResponse(res, std::chrono::steady_clock::now() - start);
}

void KeyValueServer::bulkQueryHandler(const httplib::Request& req, httplib::Response& res) {
    auto start = std::chrono::steady_clock::now();
    logRequest(req);
    nlohmann::json out;
    out["endpoint"] = "bulk_query";
    // expect JSON array of integer keys in body
    if (!req.body.empty()) {
        try {
            auto j = nlohmann::json::parse(req.body);
            if (j.is_array()) {
                nlohmann::json results = nlohmann::json::array();
                for (auto& el : j) {
                    if (el.is_number_integer()) {
                        int k = el.get<int>();
                        auto v = inline_cache.get(k);
                        results.push_back({{"key", k}, {"found", !!v}, {"value", v ? *v : nullptr}});
                    }
                }
                out["results"] = results;
                json_response(res, 200, out);
            } else {
                out["error"] = "expected array";
                json_response(res, 400, out);
            }
        } catch (const std::exception& e) {
            out["error"] = std::string("parse error: ") + e.what();
            json_response(res, 400, out);
        }
    } else {
        out["info"] = "empty body";
        json_response(res, 200, out);
    }
    logResponse(res, std::chrono::steady_clock::now() - start);
}

void KeyValueServer::insertionHandler(const httplib::Request& req, httplib::Response& res) {
    auto start = std::chrono::steady_clock::now();
    logRequest(req);
    std::string keyStr, valStr;
    if (req.path_params.count("key")) keyStr = req.path_params.at("key");
    if (req.path_params.count("value")) valStr = req.path_params.at("value");
    nlohmann::json out;
    out["key"] = keyStr;
    out["value"] = valStr;
    int key;
    if (!parse_int(keyStr, key)) {
        out["error"] = "invalid key format";
        json_response(res, 400, out);
        logResponse(res, std::chrono::steady_clock::now() - start);
        return;
    }
    bool inserted = inline_cache.insert_if_absent(key, valStr);
    if (!inserted) {
        out["error"] = "key exists";
        out["existing_value"] = inline_cache.get(key).value_or("");
        json_response(res, 409, out);
    } else {
        out["created"] = true;
        json_response(res, 201, out);
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
                json_response(res, 400, out);
            } else {
                nlohmann::json results = nlohmann::json::array();
                for (auto& op : j) {
                    nlohmann::json r;
                    if (op.contains("key") && op["key"].is_number_integer()) {
                        int k = op["key"].get<int>();
                        std::string v = op.value("value", "");
                        std::string action = op.value("op", "update_or_insert");
                        bool ok = false;
                        if (action == "insert") {
                            bool inserted = inline_cache.insert_if_absent(k, v);
                            ok = inserted;
                            if (!inserted) {
                                r["error"] = "exists";
                            }
                            r["inserted_new"] = inserted;
                        } else if (action == "update") {
                            ok = inline_cache.update(k, v);
                            if (!ok) r["error"] = "missing";
                        } else if (action == "update_or_insert") {
                            bool inserted = inline_cache.update_or_insert(k, v);
                            ok = true;
                            r["inserted_new"] = inserted;
                        } else {
                            r["error"] = "invalid op";
                        }
                        r["key"] = k;
                        r["op"] = action;
                        r["value"] = v;
                        r["success"] = ok;
                    } else {
                        r["error"] = "invalid key";
                        r["success"] = false;
                    }
                    results.push_back(r);
                }
                out["results"] = results;
                json_response(res, 200, out);
            }
        } catch (const std::exception& e) {
            out["error"] = std::string("parse error: ") + e.what();
            json_response(res, 400, out);
        }
    } else {
        out["info"] = "empty body";
        json_response(res, 200, out);
    }
    logResponse(res, std::chrono::steady_clock::now() - start);
}

void KeyValueServer::deletionHandler(const httplib::Request& req, httplib::Response& res) {
    auto start = std::chrono::steady_clock::now();
    logRequest(req);
    std::string keyStr;
    if (req.path_params.count("key")) keyStr = req.path_params.at("key");
    nlohmann::json out;
    out["key"] = keyStr;
    int key;
    if (!parse_int(keyStr, key)) {
        out["error"] = "invalid key format";
        json_response(res, 400, out);
        logResponse(res, std::chrono::steady_clock::now() - start);
        return;
    }
    auto existed = inline_cache.erase(key);
    if (existed) {
        json_response(res, 204, out);
    } else {
        out["error"] = "not found";
        json_response(res, 404, out);
    }
    logResponse(res, std::chrono::steady_clock::now() - start);
}

void KeyValueServer::updationHandler(const httplib::Request& req, httplib::Response& res) {
    auto start = std::chrono::steady_clock::now();
    logRequest(req);
    std::string keyStr, valStr;
    if (req.path_params.count("key")) keyStr = req.path_params.at("key");
    if (req.path_params.count("value")) valStr = req.path_params.at("value");
    nlohmann::json out;
    out["key"] = keyStr;
    out["value"] = valStr;
    int key;
    if (!parse_int(keyStr, key)) {
        out["error"] = "invalid key format";
        json_response(res, 400, out);
        logResponse(res, std::chrono::steady_clock::now() - start);
        return;
    }
    bool updated = inline_cache.update(key, valStr);
    if (updated) {
        out["updated"] = true;
        json_response(res, 200, out);
    } else {
        out["error"] = "not found";
        json_response(res, 404, out);
    }
    logResponse(res, std::chrono::steady_clock::now() - start);
}

void KeyValueServer::setupRoutes() {
    server_.Get("/", [this](const auto& r, auto& s) { homeHandler(r, s); });
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
    std::cout << "Http server listening at " << host_ << ":" << port_ << "\n";
    return server_.listen(host_, port_);
}

void KeyValueServer::stop() { server_.stop(); }

httplib::Server& KeyValueServer::raw() { return server_; }

// ---- Helpers ----
void KeyValueServer::json_response(httplib::Response& res, int status, const nlohmann::json& j) {
    res.status = status;
    if (status == 204) {
        res.set_content("", "application/json");
    } else {
        res.set_content(j.dump(), "application/json");
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
    json_response(res, 200, out);
    logResponse(res, std::chrono::steady_clock::now() - start);
}

void KeyValueServer::metricsHandler(const httplib::Request& req, httplib::Response& res) {
    auto start = std::chrono::steady_clock::now();
    logRequest(req);
    auto st = inline_cache.stats();
    nlohmann::json out{{"entries",st.size_entries},{"bytes",st.bytes_estimated},{"hits",st.hits},{"misses",st.misses},{"evictions",st.evictions}};
    json_response(res, 200, out);
    logResponse(res, std::chrono::steady_clock::now() - start);
}

void KeyValueServer::stopHandler(const httplib::Request& req, httplib::Response& res) {
    auto start = std::chrono::steady_clock::now();
    logRequest(req);
    nlohmann::json out{{"stopping",true}};
    json_response(res, 200, out);
    logResponse(res, std::chrono::steady_clock::now() - start);
    stop();
}
