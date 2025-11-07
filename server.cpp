// Refactored implementation: KeyValueServer class defined in server.h
#include "server.h"
#include <iostream>

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
    : host_(host), port_(port) {}

KeyValueServer::~KeyValueServer() = default;

void KeyValueServer::logRequest(const httplib::Request& req) {
    std::cout << "\n" << req.method << " request received at " << req.path << " endpoint.\n";
}

void KeyValueServer::logResponse(const httplib::Response& res) {
    std::cout << "\n{\nStatus: " << res.status
              << "\n Reason: " << res.reason
              << "\n Version: " << res.version
              << "\n Body: " << res.body
              << "\n}\n";
}

void KeyValueServer::homeHandler(const httplib::Request& req, httplib::Response& res) {
    logRequest(req);
    res.set_content(kHomePageHtml, "text/html");
    logResponse(res);
}

void KeyValueServer::getKeyHandler(const httplib::Request& req, httplib::Response& res) {
    logRequest(req);
    std::string id;
    if (req.has_param("key_id")) {
        id = req.get_param_value("key_id");
    } else if (req.path_params.count("key_id")) {
        id = req.path_params.at("key_id");
    }
    res.set_content("Query for key: " + id + " (DB integration pending)\n", "text/plain");
    logResponse(res);
}

void KeyValueServer::bulkQueryHandler(const httplib::Request& req, httplib::Response& res) {
    logRequest(req);
    res.set_content("Bulk query endpoint (DB integration pending)\n", "text/plain");
    logResponse(res);
}

void KeyValueServer::insertionHandler(const httplib::Request& req, httplib::Response& res) {
    logRequest(req);
    res.set_content(req.body.empty() ? "Insertion endpoint (body empty)\n" : req.body, "text/json");
    logResponse(res);
}

void KeyValueServer::bulkUpdateHandler(const httplib::Request& req, httplib::Response& res) {
    logRequest(req);
    res.set_content(req.body.empty() ? "Bulk update endpoint (body empty)\n" : req.body, "text/json");
    logResponse(res);
}

void KeyValueServer::deletionHandler(const httplib::Request& req, httplib::Response& res) {
    logRequest(req);
    res.set_content(req.body.empty() ? "Deletion endpoint (body empty)\n" : req.body, "text/json");
    logResponse(res);
}

void KeyValueServer::updationHandler(const httplib::Request& req, httplib::Response& res) {
    logRequest(req);
    res.set_content(req.body.empty() ? "Updation endpoint (body empty)\n" : req.body, "text/json");
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
