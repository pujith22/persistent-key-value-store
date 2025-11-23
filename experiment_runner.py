import subprocess
import time
import json
import matplotlib.pyplot as plt
import os
import sys
import signal
import argparse

import requests
import threading

# Configuration
LOAD_GEN_SCRIPT = "load_generator.py"
OPEN_LOOP_SCRIPT = "loadgen_open_loop.py"
CONCURRENCY_LEVELS = [1, 5, 10, 20, 50, 100, 200]
ARRIVAL_RATES = [10, 50, 100, 200, 500, 1000, 2000]
DURATION = 10 # Seconds per test
WORKLOADS = [1, 2, 3]
THINK_TIME = 0.05 # 50ms mean think time

RESULTS_DIR = "results"
os.makedirs(RESULTS_DIR, exist_ok=True)

def get_metrics(url):
    try:
        resp = requests.get(url, timeout=1)
        if resp.status_code == 200:
            return resp.json()
    except:
        pass
    return None

def monitor_server(monitor_url, duration, metrics_list, stop_event):
    while not stop_event.is_set():
        data = get_metrics(monitor_url)
        if data:
            metrics_list.append(data)
        time.sleep(1)

def run_load_test(url, monitor_url, workload, concurrency, duration, populate=False):
    cmd = [
        sys.executable, LOAD_GEN_SCRIPT,
        "--url", url,
        "--workload", str(workload),
        "--concurrency", str(concurrency),
        "--duration", str(duration),
        "--think-time", str(THINK_TIME)
    ]
    if populate:
        cmd.append("--populate")
        
    print(f"Running Closed Loop: Workload {workload}, Concurrency {concurrency}")
    return execute_test(cmd, monitor_url, duration)

def run_open_loop_test(url, monitor_url, workload, rate, duration, populate=False):
    cmd = [
        sys.executable, OPEN_LOOP_SCRIPT,
        "--url", url,
        "--workload", str(workload),
        "--rate", str(rate),
        "--duration", str(duration)
    ]
    if populate:
        cmd.append("--populate")
        
    print(f"Running Open Loop: Workload {workload}, Rate {rate}")
    return execute_test(cmd, monitor_url, duration)

def execute_test(cmd, monitor_url, duration):
    # Get initial metrics for counters
    initial_metrics = get_metrics(monitor_url) if monitor_url else None

    # Start monitoring for gauges
    metrics_data = []
    stop_event = threading.Event()
    monitor_thread = None
    
    if monitor_url:
        monitor_thread = threading.Thread(target=monitor_server, args=(monitor_url, duration, metrics_data, stop_event))
        monitor_thread.start()

    result = subprocess.run(cmd, capture_output=True, text=True)
    
    # Stop monitoring
    if monitor_thread:
        stop_event.set()
        monitor_thread.join()
        
    # Get final metrics for counters
    final_metrics = get_metrics(monitor_url) if monitor_url else None
    
    if result.returncode != 0:
        print("Load generator failed:")
        print(result.stderr)
        return None
        
    try:
        lines = result.stdout.strip().split('\n')
        json_str = lines[-1]
        json_start = result.stdout.find('{')
        if json_start != -1:
            json_str = result.stdout[json_start:]
            data = json.loads(json_str)
            
            # Process Server Metrics
            data['server_cpu'] = 0
            data['server_disk'] = 0
            data['server_mem_rss'] = 0
            data['cache_entries'] = 0
            data['cache_hit_ratio'] = 0
            data['disk_write_bytes_sec'] = 0
            data['disk_read_bytes_sec'] = 0
            data['disk_write_iops'] = 0
            data['disk_read_iops'] = 0
            
            if metrics_data:
                # Gauges: Average over the duration
                data['server_cpu'] = sum(m.get('cpu_utilization_percent', 0) for m in metrics_data) / len(metrics_data)
                
                # Use aggregate utilization if available, otherwise fall back to standard or 0
                data['server_disk'] = sum(m.get('disk_utilization_percent_aggregate', m.get('disk_utilization_percent', 0)) for m in metrics_data) / len(metrics_data)
                
                data['server_mem_rss'] = sum(m.get('process', {}).get('rss_kb', 0) for m in metrics_data) / len(metrics_data)
                data['cache_entries'] = sum(m.get('entries', 0) for m in metrics_data) / len(metrics_data)
                
                # Disk Throughput and IOPS
                data['disk_write_bytes_sec'] = sum(m.get('disk_write_bytes_per_sec', 0) for m in metrics_data) / len(metrics_data)
                data['disk_read_bytes_sec'] = sum(m.get('disk_read_bytes_per_sec', 0) for m in metrics_data) / len(metrics_data)
                data['disk_write_iops'] = sum(m.get('disk_write_ios_per_sec', 0) for m in metrics_data) / len(metrics_data)
                data['disk_read_iops'] = sum(m.get('disk_read_ios_per_sec', 0) for m in metrics_data) / len(metrics_data)
            
            if initial_metrics and final_metrics:
                # Counters: Delta
                hits_start = initial_metrics.get('hits', 0)
                hits_end = final_metrics.get('hits', 0)
                misses_start = initial_metrics.get('misses', 0)
                misses_end = final_metrics.get('misses', 0)
                
                delta_hits = hits_end - hits_start
                delta_misses = misses_end - misses_start
                total_reqs = delta_hits + delta_misses
                
                if total_reqs > 0:
                    data['cache_hit_ratio'] = delta_hits / total_reqs
                else:
                    data['cache_hit_ratio'] = 0
                
            return data
        else:
            print("Could not find JSON output")
            return None
    except json.JSONDecodeError as e:
        print(f"Failed to parse JSON: {e}")
        print(result.stdout)
        return None

def plot_results(all_results, mode="closed"):
    metrics = [
        'throughput', 'avg_latency', 'p95_latency', 
        'server_cpu', 'server_disk', 'server_mem_rss', 
        'cache_entries', 'cache_hit_ratio',
        'disk_write_bytes_sec', 'disk_read_bytes_sec',
        'disk_write_iops', 'disk_read_iops'
    ]
    metric_labels = {
        'throughput': 'Throughput (ops/s)',
        'avg_latency': 'Average Latency (s)',
        'p95_latency': 'P95 Latency (s)',
        'server_cpu': 'Server CPU Utilization (%)',
        'server_disk': 'Server Disk Utilization (%)',
        'server_mem_rss': 'Server Memory RSS (KB)',
        'cache_entries': 'Cache Entries',
        'cache_hit_ratio': 'Cache Hit Ratio',
        'disk_write_bytes_sec': 'Disk Write Throughput (Bytes/s)',
        'disk_read_bytes_sec': 'Disk Read Throughput (Bytes/s)',
        'disk_write_iops': 'Disk Write IOPS',
        'disk_read_iops': 'Disk Read IOPS'
    }
    
    x_axis = 'concurrency' if mode == "closed" else 'rate'
    x_label = "Concurrency" if mode == "closed" else "Arrival Rate (req/s)"
    
    for workload in WORKLOADS:
        workload_results = [r for r in all_results if r['workload'] == workload]
        workload_results.sort(key=lambda x: x[x_axis])
        
        x = [r[x_axis] for r in workload_results]
        
        for metric in metrics:
            y = [r.get(metric, 0) for r in workload_results]
            
            plt.figure()
            plt.plot(x, y, marker='o')
            plt.title(f"Workload {workload} ({mode}): {metric_labels[metric]} vs {x_label}")
            plt.xlabel(x_label)
            plt.ylabel(metric_labels[metric])
            plt.grid(True)
            plt.savefig(f"{RESULTS_DIR}/workload_{workload}_{mode}_{metric}.png")
            plt.close()

def main():
    parser = argparse.ArgumentParser(description='Experiment Runner')
    parser.add_argument('--url', type=str, default="http://localhost:2222", help='Server URL (e.g., http://localhost:2222)')
    parser.add_argument('--monitor-url', type=str, help='Server Monitor URL (default: url + /metrics)')
    parser.add_argument('--mode', type=str, choices=['closed', 'open', 'all'], default='all', help='Test mode')
    args = parser.parse_args()
    
    # Default monitor URL if not provided
    monitor_url = args.monitor_url
    if not monitor_url:
        monitor_url = f"{args.url}/metrics"
    
    print(f"Target URL: {args.url}")
    print(f"Monitor URL: {monitor_url}")
    
    if args.mode in ['closed', 'all']:
        print("\n=== Starting Closed Loop Experiments ===")
        closed_results = []
        for workload in WORKLOADS:
            print(f"\n--- Workload {workload} ---")
            for concurrency in CONCURRENCY_LEVELS:
                res = run_load_test(args.url, monitor_url, workload, concurrency, DURATION, populate=True)
                if res:
                    closed_results.append(res)
                    print(f"Result: {res['throughput']:.2f} ops/s, Latency: {res['avg_latency']:.4f}s, CPU: {res['server_cpu']:.1f}%, Disk: {res['server_disk']:.1f}%, Hits: {res['cache_hit_ratio']:.2f}")
                time.sleep(1)
        
        with open(f"{RESULTS_DIR}/closed_loop_results.json", "w") as f:
            json.dump(closed_results, f, indent=2)
        plot_results(closed_results, "closed")

    if args.mode in ['open', 'all']:
        print("\n=== Starting Open Loop Experiments ===")
        open_results = []
        for workload in WORKLOADS:
            print(f"\n--- Workload {workload} ---")
            for rate in ARRIVAL_RATES:
                res = run_open_loop_test(args.url, monitor_url, workload, rate, DURATION, populate=True)
                if res:
                    open_results.append(res)
                    print(f"Result: {res['throughput']:.2f} ops/s, Latency: {res['avg_latency']:.4f}s, CPU: {res['server_cpu']:.1f}%, Disk: {res['server_disk']:.1f}%, Hits: {res['cache_hit_ratio']:.2f}")
                time.sleep(1)
                
        with open(f"{RESULTS_DIR}/open_loop_results.json", "w") as f:
            json.dump(open_results, f, indent=2)
        plot_results(open_results, "open")

if __name__ == "__main__":
    main()
