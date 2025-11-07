# Persistent Key-Value Store

A high-performance, persistent key-value store implemented in C++ with support and REST API over HTTP.

## Features

- **Persistence Backend**
  - PostgreSQL
  - In-Memory (for testing/development)
- **REST API over HTTP** - (Will be upgraded later to HTTPS for Secure communication with SSL/TLS)
- **In-Memory Caching** - Fast access with write-through cache
- **Thread-Safe Operations** - Concurrent request handling
- **Integer Keys & String Values** - Simple yet extensible data model
- **Command-Line Configuration** - Flexible backend and connection settings

## Architecture (The initial Plan)

```
┌─────────────┐
│ REST Client │
└──────┬──────┘
       │ HTTPS
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
```

## Contributions

Author: Pujith Sai Kumar Korlepara (@pujith22)

Contributions are welcome! Please feel free to submit a Pull Request.

## License

See LICENSE file for details.