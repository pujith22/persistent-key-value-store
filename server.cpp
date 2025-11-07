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
    "</table> </html>";

KeyValueServer::KeyValueServer(const std::string& host, int port)
    : host_(host), port_(port), cache_(InlineCache::Policy::LRU) {}

KeyValueServer::KeyValueServer(const std::string& host, int port, InlineCache::Policy policy)
    : host_(host), port_(port), cache_(policy) {}

KeyValueServer::~KeyValueServer() = default;

void KeyValueServer::logRequest(const httplib::Request& req) {
    std::cout << "[REQUEST] method=" << req.method << " path=" << req.path << "\n";
}

void KeyValueServer::logResponse(const httplib::Response& res) {
    std::cout << "[RESPONSE] status=" << res.status << " reason=" << res.reason
              << " ct=" << res.get_header_value("Content-Type")
              << " bytes=" << res.body.size() << "\n";
}

void KeyValueServer::homeHandler(const httplib::Request& req, httplib::Response& res) {
    logRequest(req);
    res.status = 200;
    res.set_content(kHomePageHtml, "text/html");
    logResponse(res);
}

void KeyValueServer::getKeyHandler(const httplib::Request& req, httplib::Response& res) {
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
        logResponse(res);
        return;
    }
    auto v = cache_.get(key);
    if (v) {
        out["found"] = true;
        out["value"] = *v;
        json_response(res, 200, out);
    } else {
        out["found"] = false;
        json_response(res, 404, out);
    }
    logResponse(res);
}

void KeyValueServer::bulkQueryHandler(const httplib::Request& req, httplib::Response& res) {
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
                        auto v = cache_.get(k);
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
    logResponse(res);
}

void KeyValueServer::insertionHandler(const httplib::Request& req, httplib::Response& res) {
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
        logResponse(res);
        return;
    }
    auto existing = cache_.get(key);
    if (existing) {
        out["error"] = "key exists";
        out["existing_value"] = *existing;
        json_response(res, 409, out);
    } else {
        cache_.insert_if_absent(key, valStr);
        out["created"] = true;
        json_response(res, 201, out);
    }
    logResponse(res);
}

void KeyValueServer::bulkUpdateHandler(const httplib::Request& req, httplib::Response& res) {
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
                        std::string action = op.value("op", "upsert");
                        bool ok=false;
                        if (action == "insert") {
                            if (!cache_.get(k)) { cache_.insert_if_absent(k, v); ok=true; } else { r["error"] = "exists"; }
                        } else if (action == "update") {
                            ok = cache_.update(k, v); if (!ok) r["error"] = "missing";
                        } else { // upsert
                            ok = !cache_.upsert(k, v); // upsert returns false if updated existing
                        }
                        r["key"] = k;
                        r["op"] = action;
                        r["success"] = ok;
                    } else {
                        r["error"] = "invalid key";
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
    logResponse(res);
}

void KeyValueServer::deletionHandler(const httplib::Request& req, httplib::Response& res) {
    logRequest(req);
    std::string keyStr;
    if (req.path_params.count("key")) keyStr = req.path_params.at("key");
    nlohmann::json out;
    out["key"] = keyStr;
    int key;
    if (!parse_int(keyStr, key)) {
        out["error"] = "invalid key format";
        json_response(res, 400, out);
        logResponse(res);
        return;
    }
    auto existed = cache_.erase(key);
    if (existed) {
        json_response(res, 204, out);
    } else {
        out["error"] = "not found";
        json_response(res, 404, out);
    }
    logResponse(res);
}

void KeyValueServer::updationHandler(const httplib::Request& req, httplib::Response& res) {
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
        logResponse(res);
        return;
    }
    bool updated = cache_.update(key, valStr);
    if (updated) {
        out["updated"] = true;
        json_response(res, 200, out);
    } else {
        out["error"] = "not found";
        json_response(res, 404, out);
    }
    logResponse(res);
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
    server_.Get("/stop", [this](const auto& r, auto& s) { logRequest(r); stop(); logResponse(s); });
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
    res.set_content(j.dump(), "application/json");
}

bool KeyValueServer::parse_int(const std::string& s, int& out) {
    try {
        size_t idx=0; int v = std::stoi(s, &idx); if (idx != s.size()) return false; out = v; return true;
    } catch (...) { return false; }
}
