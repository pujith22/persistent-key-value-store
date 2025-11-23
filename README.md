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

### Configuration & runtime flags

You can configure where the server listens using a simple `.env` file placed at the project root or by setting environment variables directly. Example `.env` (already included as `.env` in the repo):

```
SERVER_HOST=0.0.0.0
SERVER_PORT=2222
```

The server will read `SERVER_HOST` and `SERVER_PORT` from the environment at startup. Command-line flags take precedence for other options (see below).

New CLI flags
- `--no-preload` or `--skip-preload` — skip preloading keys from persistence into the inline cache during startup. By default the server synchronously preloads keys 1..1000 from the database into the inline cache before it begins accepting connections (this can increase startup time but reduces cold-cache misses).

Connection pooling and async DB worker pool
- The persistence adapter now maintains a pool of libpq connections and an internal worker thread pool to offload blocking database operations. This reduces HTTP worker thread starvation under heavy load.
- Prepared statements required by the adapter are created on each pooled connection at startup. Connections that fail to prepare are dropped and exposed via pool metrics.

Upgraded `/metrics` end point
- The `/metrics` end currently returns a JSON document containing cache stats and persistence pool metrics. It also exposes several system-level metrics useful for stress tests and load generators. Key fields include:
       - `cpu_utilization_percent` — CPU busy percentage since the last `/metrics` sample (kernel counters based)
       - `memory_kb` — object with `total`, `free`, and `available` (in kB)
       - `disk_read_bytes`, `disk_write_bytes` — cumulative bytes read/written across block devices (derived from `/sys/block/*/stat` sectors; converted assuming 512B sectors)
       - `disk_io_ops` — object with `read_ios` and `write_ios` counts
       - `network_bytes` — object with `rx` and `tx` cumulative bytes (aggregated from `/proc/net/dev`, non-loopback)
       - `persistence_pool` — connection pool counters and stats (dropped connections, total creates, free connections, etc.)

Examples
- Start normally (preload enabled):

```sh
./kv_server.out --json-logs --policy=lru
```

- Start and skip preloading (faster startup):

```sh
./kv_server.out --json-logs --policy=lru --no-preload
```

Preload behavior
- When preload is enabled (default), the server attempts to fetch keys in the range 1..1000 from the persistence layer and populate the inline cache with any found values. Progress is logged (every 100 keys) and a final preload summary is included in the startup log.
- When preload is disabled with `--no-preload`, the server starts listening immediately and the cache will be populated on demand.

Insertion helper script
- A convenience script is included at `scripts/insert_random_kv.sh` to populate the database with test data. It inserts integer keys in a configurable range and random string values. It uses `INSERT ... ON CONFLICT DO NOTHING` so existing keys are not overwritten.

Example:

```sh
PG_CONNINFO='dbname=kvstore user=pujith22 password=...' ./scripts/insert_random_kv.sh --start 1 --end 1000 --min-len 8 --max-len 256
```


## Testing

Use the fake persistence provider to exercise the HTTP surface without PostgreSQL:

```sh
# Build unit/integration tests WITHOUT a PostgreSQL client (recommended for fast local runs)
# This uses the test-only persistence adapter stub in `test/persistence_adapter_stub.cpp`.
g++ -std=c++17 test/test_server.cpp server.cpp test/persistence_adapter_stub.cpp \
       -I include -I third_party -lpthread -o test_server.out
./test_server.out

# There are also specialized tests. Example: metrics test that validates /metrics JSON
g++ -std=c++17 test/test_metrics.cpp server.cpp test/persistence_adapter_stub.cpp \
       -I include -I third_party -lpthread -o test_metrics.out
./test_metrics.out
```

Full integration tests that exercise the real persistence adapter require PostgreSQL client headers/libpq and a reachable DB. See `build_instruction.txt` for environment hints and the `scripts/setup_pg_env.zsh` helper.

The test suite validates transactional semantics (including rollback on the first failure), bulk query robustness, cache integration, and the presence/types of the new system and process-level metrics.

Load testing notes
- Use `scripts/insert_random_kv.sh` to populate the database before starting a load test.
- Scrape `/metrics` at a steady interval (for example every 5s) to collect CPU, memory, disk and network data points alongside your request/response metrics. CPU utilization is computed from kernel counters between successive `/metrics` calls — scrape interval affects resolution.
- Disk bytes reported are cumulative since boot (derived from sectors). If you need per-second rates, you can compute deltas between successive `/metrics` samples or request an update to include per-second rate fields.

### Automated Experiments

Use `experiment_runner.py` to run a full suite of closed-loop and open-loop load tests across different concurrency levels and arrival rates.

```sh
# Run all experiments (closed and open loop)
python3 experiment_runner.py --url http://localhost:2222 --mode all

# Run only closed-loop experiments
python3 experiment_runner.py --url http://localhost:2222 --mode closed

# Run only open-loop experiments
python3 experiment_runner.py --url http://localhost:2222 --mode open
```

The script will:

1. Run tests for Workloads 1, 2, and 3.
2. Collect server-side metrics (CPU, Disk, Memory) and client-side metrics (Throughput, Latency).
3. Generate plots in the `results/` directory.

### Saturation Tests

We provide specialized scripts to stress test specific system resources:

**CPU Saturation Test**
Target: Saturate server CPU by flooding it with GET requests for a small set of hot keys (1-100).

```sh
python3 cpu_saturation_test.py --start-rate 100 --step 500 --max-rate 10000
```

**Disk Saturation Test**
Target: Saturate disk I/O by flooding it with write operations (POST/PUT) using larger payloads (1KB).

```sh
python3 disk_saturation_test.py --start-rate 100 --step 100 --max-rate 2000
```

This script generates plots for Disk Utilization, Write Throughput, and Latency in `results_disk_saturation/`.

Example: poll `/metrics` every 5 seconds using the included helper script (requires `jq`):

```sh
./scripts/poll_metrics.sh http://localhost:2222/metrics 5
```

The script prints a single-line summary per poll including CPU percent, memory available/total, disk read/write bytes per second and network rx/tx bytes per second.

## Metrics fields

The `/metrics` endpoint returns a JSON object combining cache stats, persistence pool metrics, system metrics and process metrics. Below are the fields and how to interpret them:

- Cache metrics
       - `entries` : integer — number of entries currently in the inline cache.
       - `bytes` : integer — estimated bytes consumed by cached entries.
       - `hits` : integer — cumulative cache hits.
       - `misses` : integer — cumulative cache misses.
       - `evictions` : integer — cumulative eviction count.

- Persistence pool
       - `persistence_pool` : object — connection pool counters returned by the adapter:
              - `pool_size` : configured pool size (int)
              - `free_conns` : number of currently free/connections available (int)
              - `dropped_conns` : number of connections dropped due to prepare/connect failures (int)
              - `total_conn_creates` : total number of connections created (int)
              - `total_conn_create_failures` : total connection create failures (int)

- CPU & memory
       - `cpu_utilization_percent` : double — percent busy since the last `/metrics` sample (kernel jiffies based). This is an average across all CPUs computed from /proc/stat.
       - `memory_kb` : object — memory snapshot in kilobytes:
              - `total` : total RAM (kB)
              - `free` : free RAM (kB)
              - `available` : memory available for new processes (kB)

- Disk (I/O activity)
       - `disk_read_bytes`, `disk_write_bytes` : unsigned integer — cumulative bytes read/written across block devices since boot (derived from sectors reported in /sys/block/*/stat). Note: sector->byte uses 512B by default; if you need exact values we can read per-device `hw_sector_size`.
       - `disk_io_ops` : object — cumulative I/O operation counts:
              - `read_ios` : number of read I/O completions
              - `write_ios` : number of write I/O completions
       - `disk_read_bytes_per_sec`, `disk_write_bytes_per_sec` : double — rate computed between successive metrics samples (bytes/sec)
       - `disk_read_ios_per_sec`, `disk_write_ios_per_sec` : double — IOPS rate (ops/sec)
       - `disk_utilization_percent_avg_per_device` : double — average busy percentage per reported device (0..100%). Computed as device-ms / (elapsed_ms * device_count).
       - `disk_utilization_percent_aggregate` : double — aggregate device busy percent (total device-ms / elapsed_ms * 100). This is the overall device-time fraction and can exceed 100% for multi-device systems (e.g., two fully busy devices -> ~200%). Use this as the overall "how busy" signal.
       - `disk_devices_reported` : int — number of block devices sampled under /sys/block.

- Network
       - `network_bytes` : object — cumulative bytes:
              - `rx` : bytes received since boot
              - `tx` : bytes transmitted since boot
       - `network_rx_bytes_per_sec`, `network_tx_bytes_per_sec` : double — rates computed between successive metrics samples (bytes/sec)

- Process-level
       - `process` : object — process-specific metrics for the server process:
              - `vms_kb` : virtual memory size (kB)
              - `rss_kb` : resident set size (kB)
              - `threads` : number of threads in the process
              - `open_fds` : number of open file descriptors

Notes & interpretation
- For load testing on SSDs, prefer bytes/sec and IOPS (disk_read_bytes_per_sec, disk_read_ios_per_sec). Disk time-busy will often be low even under high throughput because SSDs are low-latency devices.
- `disk_utilization_percent_aggregate` is the recommended single-number "overall disk activity" metric (it sums device-ms and is independent of the number of devices). Expect values >100% on multi-device systems.


## Credits

- [nlohmann/json](https://github.com/nlohmann/json) — Niels Lohmann
- [cpp-httplib](https://github.com/yhirose/cpp-httplib) — Yuji Hirose

## Author & Contact

| Name | Institute | Email | GitHub | LinkedIn | Portfolio |
|------|-----------|-------|--------|----------|-----------|
| Pujith Sai Kumar Korlepara | IIT Bombay (ID: 25M0787) | <pujith@cse.iitb.ac.in> / <pujith22.sde@gmail.com> | [Profile](https://github.com/pujith22) | [Profile](https://www.linkedin.com/in/pujith22) | [Portfolio](https://www.cse.iitb.ac.in/~pujith) |

Additional contact: WhatsApp/Telegram +91 9996159269

## Postman API Collection

<https://www.postman.com/altimetry-architect-63208177/persistent-key-value-server-public/collection/f47ai0l/persistent-key-value-server?action=share&creator=18466773>

## License

This project is licensed under the terms of the MIT License. See `LICENSE` for details.
