#!/usr/bin/env python3
"""
Closed-loop load generator.
Spawns N client threads; each thread repeatedly issues requests (according to workload) sequentially
for the specified duration. Records per-request latencies and statuses and reports throughput and
latency percentiles.

Usage:
  python3 loadgen_closed_loop.py --url http://localhost:2222 --concurrency 10 --duration 60 --workload read

Workload options: 'read' (GET /get_key/:key_id), 'write' (POST /insert/:key/:value), 'mixed'
"""

import argparse
import threading
import time
import requests
import random
import statistics
import json
from collections import defaultdict


def worker_loop(worker_id, stop_at, url, workload, stats, key_space=1000):
    s = requests.Session()
    local_latencies = []
    local_success = 0
    local_fail = 0
    while time.time() < stop_at:
        try:
            if workload == 'read':
                key = random.randint(1, key_space)
                start = time.time()
                r = s.get(f"{url}/get_key/{key}")
                latency = time.time() - start
            elif workload == 'write':
                key = random.randint(1, key_space)
                val = f"v{random.randint(1,1000000)}"
                start = time.time()
                r = s.post(f"{url}/insert/{key}/{val}")
                latency = time.time() - start
            else:  # mixed
                if random.random() < 0.7:
                    key = random.randint(1, key_space)
                    start = time.time()
                    r = s.get(f"{url}/get_key/{key}")
                    latency = time.time() - start
                else:
                    key = random.randint(1, key_space)
                    val = f"v{random.randint(1,1000000)}"
                    start = time.time()
                    r = s.post(f"{url}/insert/{key}/{val}")
                    latency = time.time() - start

            local_latencies.append(latency)
            if 200 <= r.status_code < 300:
                local_success += 1
            else:
                local_fail += 1
        except Exception:
            local_fail += 1
    # push local stats to shared
    with stats['lock']:
        stats['latencies'].extend(local_latencies)
        stats['success'] += local_success
        stats['fail'] += local_fail


def run_closed_loop(url, concurrency, duration, workload):
    stop_at = time.time() + duration
    stats = {'latencies': [], 'success': 0, 'fail': 0, 'lock': threading.Lock()}
    threads = []
    for i in range(concurrency):
        t = threading.Thread(target=worker_loop, args=(i, stop_at, url, workload, stats))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()

    # compute metrics
    lat = stats['latencies']
    total_ops = stats['success'] + stats['fail']
    throughput = total_ops / duration
    result = {
        'concurrency': concurrency,
        'duration_s': duration,
        'workload': workload,
        'total_ops': total_ops,
        'success': stats['success'],
        'fail': stats['fail'],
        'throughput_ops_s': throughput,
    }
    if lat:
        result.update({
            'latency_mean_s': statistics.mean(lat),
            'latency_median_s': statistics.median(lat),
            'latency_p95_s': sorted(lat)[int(len(lat)*0.95)-1],
            'latency_p99_s': sorted(lat)[int(len(lat)*0.99)-1] if len(lat) >= 100 else None,
        })
    else:
        result.update({'latency_mean_s': None, 'latency_median_s': None, 'latency_p95_s': None, 'latency_p99_s': None})
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--url', required=True, help='Base URL of server, e.g. http://localhost:2222')
    parser.add_argument('--concurrency', type=int, default=10)
    parser.add_argument('--duration', type=int, default=60, help='Duration in seconds')
    parser.add_argument('--workload', choices=['read', 'write', 'mixed'], default='read')
    args = parser.parse_args()

    res = run_closed_loop(args.url, args.concurrency, args.duration, args.workload)
    print(json.dumps(res, indent=2))
