#pragma once

#include <string>
#include <cstdlib>
#include <fstream>
#include <sstream>
#include <iostream>
#include "nlohmann/json.hpp"  // Prefer vendored header from third_party/nlohmann/json.hpp


// load_conninfo tries the following, in order:
// 1) Environment variable PG_CONNINFO
// 2) JSON file at config/db.json with a key: { "conninfo": "..." }
// 3) Provided defaultValue (defaults to "dbname=kvstore")

inline std::string load_conninfo(const std::string &defaultValue = "dbname=kvstore") {

    //  reeading from config/db.json (full-fledged JSON parsing via nlohmann/json)
    try {
        std::ifstream in("config/db.json");
        if (in) {
            nlohmann::json j;
            in >> j;
            if (j.contains("conninfo") && j["conninfo"].is_string()) {
                return j["conninfo"].get<std::string>();
            }
        }
    } catch (...) {
        std::cout<<"Error while reading config/db.json file.";
    }

    // using default value provided
    return defaultValue;
}
