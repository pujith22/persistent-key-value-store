# Persistent Key-Value Store

C++17 HTTP service that exposes a persistent key-value cache backed by PostgreSQL. The server enforces persistence availability at startup, streams structured logs, and keeps hot keys in an inline cache for low-latency reads.

## Highlights

- **Mandatory persistence**: startup fails fast if the configured PostgreSQL backend cannot be reached.
- **Rich HTTP API**: JSON-driven endpoints for lookups, bulk queries, transactional updates, and cache-aware deletes.
- **Write-through inline cache**: integer keys and string values served from memory with automatic hydration from persistence.
- **Configurable policies**: LRU, FIFO, or Random eviction with cache size monitoring (target footprint ~2 MB).
- **Structured observability**: optional JSON request/response logging with latency metrics.

## Architecture Overview

```text
┌─────────────┐
│ REST Client │
└──────┬──────┘
       │ HTTP
┌──────▼──────────┐
│  HTTP Server    │   cpp-httplib
└──────┬──────────┘
       │
┌──────▼──────────┐
│ Inline Cache    │   in-memory write-through
└──────┬──────────┘
       │
┌──────▼──────────┐
│ Persistence     │   adapter abstraction
└──────┬──────────┘
       │
┌──────▼──────────┐
│ PostgreSQL DB   │
└─────────────────┘
```

## REST Endpoints

| Method | Path                   | Description                                      |
|--------|------------------------|--------------------------------------------------|
| GET    | `/`                    | Service metadata and route catalog               |
| GET    | `/home`                | HTML dashboard with endpoint references          |
| GET    | `/get_key/:key_id`     | Lookup a single integer key                      |
| PATCH  | `/bulk_query`          | Retrieve multiple keys in one request            |
| POST   | `/insert/:key/:value`  | Insert a new key/value pair (409 on conflict)    |
| POST   | `/bulk_update`         | Transactional pipeline for insert/update/delete  |
| DELETE | `/delete_key/:key`     | Remove a key from cache and persistence          |
| PUT    | `/update_key/:key/:value` | Update an existing key with a new value       |
| GET    | `/health`              | Uptime and status metrics                        |
| GET    | `/metrics`             | Cache hit/miss counters                          |
| GET    | `/stop`                | Graceful shutdown (testing only)                 |

All endpoints return JSON responses with a `reason` field for traceability. `/bulk_update` always runs in transactional mode and marks `success=false` if any operation fails.

## Getting Started

### 1. Install dependencies

- PostgreSQL server and libpq headers.
- C++17 toolchain (g++ or clang++).

### 2. Configure persistence

- Provide a connection string via the `PG_CONNINFO` environment variable or `config/db.json`.

### 3. Build the server

```sh
g++ -std=c++17 server.cpp main_server.cpp persistence_adapter.cpp \
    -I include -I third_party -I"$(pg_config --includedir)" \
    -L"$(pg_config --libdir)" -lpq -o kv_server.out
```

### 4. Run

```sh
./kv_server.out --json-logs --policy=lru
```

The process exits with status `1` if persistence is unreachable.

## Testing

Use the fake persistence provider to exercise the HTTP surface without PostgreSQL:

```sh
g++ -std=c++17 test/test_server.cpp server.cpp -I include -I third_party -o test_server.out
./test_server.out
```

The test suite validates transactional semantics (including rollback on the first failure), bulk query robustness, and cache integration.

## Credits

- [nlohmann/json](https://github.com/nlohmann/json) — Niels Lohmann
- [cpp-httplib](https://github.com/yhirose/cpp-httplib) — Yuji Hirose

## Author & Contact

| Name | Institute | Email | GitHub | LinkedIn | Portfolio |
|------|-----------|-------|--------|----------|-----------|
| Pujith Sai Kumar Korlepara | IIT Bombay (ID: 25M0787) | <pujith@cse.iitb.ac.in> / <pujith22.sde@gmail.com> | [Profile](https://github.com/pujith22) | [Profile](https://www.linkedin.com/in/pujith22) | [Portfolio](https://www.cse.iitb.ac.in/~pujith) |

Additional contact: WhatsApp/Telegram +91 9996159269

## License

This project is licensed under the terms of the MIT License. See `LICENSE` for details.
