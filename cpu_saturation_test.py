import subprocess
import sys
import time
import json
import requests
import threading
import argparse

OPEN_LOOP_SCRIPT = "loadgen_open_loop.py"

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
        "--workload", "4",  # Use the new CPU saturation workload
        "--rate", str(rate),
        "--duration", str(duration)
    ]
    
    # print(f"Testing Rate: {rate} req/s")
    
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
        # print(f"Test failed for rate {rate}")
        # print(result.stderr)
        return None, 0
        
    try:
        lines = result.stdout.strip().split('\n')
        json_str = lines[-1]
        json_start = result.stdout.find('{')
        if json_start != -1:
            json_str = result.stdout[json_start:]
            data = json.loads(json_str)
            
            avg_cpu = 0
            if metrics_data:
                avg_cpu = sum(m.get('cpu_utilization_percent', 0) for m in metrics_data) / len(metrics_data)
            
            return data, avg_cpu
    except Exception as e:
        print(f"Error parsing result: {e}")
        print(result.stdout)
        
    return None, 0

def main():
    parser = argparse.ArgumentParser(description='CPU Saturation Test')
    parser.add_argument('--url', type=str, default="http://localhost:8080", help='Server URL')
    parser.add_argument('--monitor-url', type=str, default="http://localhost:5000/metrics", help='Monitor URL')
    parser.add_argument('--start-rate', type=int, default=100, help='Starting RPS')
    parser.add_argument('--step', type=int, default=500, help='RPS step increase')
    parser.add_argument('--max-rate', type=int, default=10000, help='Max RPS')
    parser.add_argument('--duration', type=int, default=10, help='Duration per step')
    
    args = parser.parse_args()
    
    current_rate = args.start_rate
    
    print("Starting CPU Saturation Test (Workload 4: GET 1-100)")
    print(f"Target: Saturate CPU (>95%) or reach {args.max_rate} RPS")
    print("-" * 75)
    print(f"{'Rate (req/s)':<15} | {'Throughput':<15} | {'Avg Latency (s)':<15} | {'CPU Util (%)':<15} | {'Success':<10}")
    print("-" * 75)
    
    while current_rate <= args.max_rate:
        data, cpu_util = run_test(args.url, args.monitor_url, current_rate, args.duration)
        
        if data:
            print(f"{current_rate:<15} | {data['throughput']:<15.2f} | {data['avg_latency']:<15.4f} | {cpu_util:<15.2f} | {data['success']:<10}")
            
            if cpu_util > 95:
                print("-" * 75)
                print(f"CPU Saturated at ~{current_rate} req/s (CPU: {cpu_util:.2f}%)")
                break
        else:
            print(f"{current_rate:<15} | {'FAILED':<15} | {'-':<15} | {'-':<15} | {'-'}")
            
        current_rate += args.step
        time.sleep(2) # Cooldown

if __name__ == "__main__":
    main()
