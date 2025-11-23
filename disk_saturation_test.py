import subprocess
import sys
import time
import json
import requests
import threading
import argparse
import matplotlib.pyplot as plt
import os

OPEN_LOOP_SCRIPT = "loadgen_open_loop.py"
RESULTS_DIR = "results_disk_saturation"
os.makedirs(RESULTS_DIR, exist_ok=True)

def get_metrics(url):
    try:
        resp = requests.get(url, timeout=1)
        if resp.status_code == 200:
            return resp.json()
    except:
        pass
    return None

def monitor_server(monitor_url, metrics_list, stop_event):
    while not stop_event.is_set():
        data = get_metrics(monitor_url)
        if data:
            metrics_list.append(data)
        time.sleep(1)

def run_test(url, monitor_url, rate, duration):
    cmd = [
        sys.executable, OPEN_LOOP_SCRIPT,
        "--url", url,
        "--workload", "5",  # Workload 5: Disk Saturation (Writes)
        "--rate", str(rate),
        "--duration", str(duration)
    ]
    
    metrics_data = []
    stop_event = threading.Event()
    monitor_thread = None
    
    if monitor_url:
        monitor_thread = threading.Thread(target=monitor_server, args=(monitor_url, metrics_data, stop_event))
        monitor_thread.start()
        
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if monitor_thread:
        stop_event.set()
        monitor_thread.join()
        
    if result.returncode != 0:
        return None, 0, 0
        
    try:
        lines = result.stdout.strip().split('\n')
        json_str = lines[-1]
        json_start = result.stdout.find('{')
        if json_start != -1:
            json_str = result.stdout[json_start:]
            data = json.loads(json_str)
            
            avg_disk_util = 0
            avg_write_bytes = 0
            
            if metrics_data:
                # Prefer aggregate utilization, fallback to standard
                avg_disk_util = sum(m.get('disk_utilization_percent_aggregate', m.get('disk_utilization_percent', 0)) for m in metrics_data) / len(metrics_data)
                avg_write_bytes = sum(m.get('disk_write_bytes_per_sec', 0) for m in metrics_data) / len(metrics_data)
            
            return data, avg_disk_util, avg_write_bytes
    except Exception as e:
        print(f"Error parsing result: {e}")
        
    return None, 0, 0

def plot_results(results):
    rates = [r['rate'] for r in results]
    
    # Plot 1: Rate vs Disk Utilization
    plt.figure(figsize=(10, 6))
    plt.plot(rates, [r['disk_util'] for r in results], marker='o', color='r')
    plt.title("Disk Saturation: Rate vs Disk Utilization")
    plt.xlabel("Request Rate (req/s)")
    plt.ylabel("Disk Utilization (%)")
    plt.grid(True)
    plt.savefig(f"{RESULTS_DIR}/disk_utilization.png")
    plt.close()
    
    # Plot 2: Rate vs Write Throughput (MB/s)
    plt.figure(figsize=(10, 6))
    plt.plot(rates, [r['write_bytes'] / 1024 / 1024 for r in results], marker='o', color='b')
    plt.title("Disk Saturation: Rate vs Write Throughput")
    plt.xlabel("Request Rate (req/s)")
    plt.ylabel("Write Throughput (MB/s)")
    plt.grid(True)
    plt.savefig(f"{RESULTS_DIR}/disk_throughput.png")
    plt.close()

    # Plot 3: Rate vs Latency
    plt.figure(figsize=(10, 6))
    plt.plot(rates, [r['avg_latency'] * 1000 for r in results], marker='o', color='g')
    plt.title("Disk Saturation: Rate vs Latency")
    plt.xlabel("Request Rate (req/s)")
    plt.ylabel("Avg Latency (ms)")
    plt.grid(True)
    plt.savefig(f"{RESULTS_DIR}/latency.png")
    plt.close()

def main():
    parser = argparse.ArgumentParser(description='Disk Saturation Test')
    parser.add_argument('--url', type=str, default="http://localhost:8080", help='Server URL')
    parser.add_argument('--monitor-url', type=str, default="http://localhost:5000/metrics", help='Monitor URL')
    parser.add_argument('--start-rate', type=int, default=100, help='Starting RPS')
    parser.add_argument('--step', type=int, default=100, help='RPS step increase')
    parser.add_argument('--max-rate', type=int, default=2000, help='Max RPS')
    parser.add_argument('--duration', type=int, default=10, help='Duration per step')
    
    args = parser.parse_args()
    
    current_rate = args.start_rate
    results = []
    
    print("Starting Disk Saturation Test (Workload 5: 100% Writes, 1KB values)")
    print("-" * 95)
    print(f"{'Rate (req/s)':<15} | {'Throughput':<15} | {'Avg Latency (s)':<15} | {'Disk Util (%)':<15} | {'Write (MB/s)':<15}")
    print("-" * 95)
    
    while current_rate <= args.max_rate:
        data, disk_util, write_bytes = run_test(args.url, args.monitor_url, current_rate, args.duration)
        
        if data:
            write_mb = write_bytes / 1024 / 1024
            print(f"{current_rate:<15} | {data['throughput']:<15.2f} | {data['avg_latency']:<15.4f} | {disk_util:<15.2f} | {write_mb:<15.2f}")
            
            results.append({
                'rate': current_rate,
                'throughput': data['throughput'],
                'avg_latency': data['avg_latency'],
                'disk_util': disk_util,
                'write_bytes': write_bytes
            })
            
            # Stop if disk is saturated (e.g., > 90% util)
            # Note: Aggregate util can go > 100% on multi-disk, but let's just warn or keep going.
            # If it's single disk, 100% is max.
            if disk_util > 95:
                 print(f"Disk potentially saturated at {disk_util:.2f}%")
                 # We might want to continue a bit to see latency spike
        else:
            print(f"{current_rate:<15} | {'FAILED':<15} | {'-':<15} | {'-':<15} | {'-'}")
            
        current_rate += args.step
        time.sleep(2) # Cooldown

    print(f"\nTest complete. Generating plots in {RESULTS_DIR}...")
    plot_results(results)
    print("Done.")

if __name__ == "__main__":
    main()
