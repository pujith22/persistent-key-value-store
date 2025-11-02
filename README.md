# Persistent Key-Value Store

A high-performance, persistent key-value store implemented in C++ with support for multiple database backends and REST API over HTTPS.

## Features

- **Multiple Persistence Backends**
  - MongoDB (default)
  - PostgreSQL
  - In-Memory (for testing/development)
- **REST API over HTTPS** - Secure communication with SSL/TLS
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
│ MongoDB/PostgreSQL │
└─────────────────┘
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

See LICENSE file for details.