# Persistent Key-Value Store

A high-performance, persistent key-value store implemented in C++ with support and REST API over HTTP.

## Features

- **Persistence Backend (Required)**
       - PostgreSQL (the server exits gracefully if the database is unavailable)
       - For automated tests a fake persistence provider is injected, but production runs must provide a real database connection.
- **REST API over HTTP** - (Will be upgraded later to HTTPS for Secure communication with SSL/TLS)
- **In-Memory Caching** - Fast access with write-through cache
- **Thread-Safe Operations** - Concurrent request handling
- **Integer Keys & String Values** - Simple yet extensible data model
- **Command-Line Configuration** - Flexible backend and connection settings

## Architecture (The initial Plan)

```text
┌─────────────┐
│ REST Client │
└──────┬──────┘
       │ HTTP
┌──────▼──────────┐
│  REST Server    │
│  (cpp-httplib)  │
└──────┬──────────┘
       │
┌──────▼──────────┐
│    KV Store     │
│  (with cache)   │
└──────┬──────────┘
       │
┌──────▼──────────┐
│   Persistence   │
│     Adapter     │
└──────┬──────────┘
       │
┌──────▼──────────┐
│    PostgreSQL   │
└─────────────────┘

Cache eviction policy include LRU, FIFO and Random Eviction limiting cache size to ~ 2 MB, by thoughtfully monitoring the inmemory cache size.
```

## Special Credits

Niels Lohmann [@nlohmann.me](https://nlohmann.me/) (for nlohmann/json library [github.com/nlohmann/json](https://github.com/nlohmann/json))
Yuji Hirose (for httplib.h library [github.com/yhirose/cpp-httplib](https://github.com/yhirose/cpp-httplib))


## Startup Requirements

- Ensure a valid PostgreSQL connection string is available via `PG_CONNINFO` or `config/db.json` before launching the server. Startup logging will emit a failure message and the process will exit with status code `1` if the database cannot be reached.

## Contributions

Author: Pujith Sai Kumar Korlepara (@pujith22)

Contributions are welcome! Please feel free to submit a Pull Request.

## License

See LICENSE file for details.
