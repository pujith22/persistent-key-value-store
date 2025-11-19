import http.client
import json
import time
import random
import concurrent.futures
import statistics
import sys
import threading
import psutil
import argparse
from collections import defaultdict
from urllib.parse import urlparse

HOST = "localhost"
PORT = 2222

class SystemMonitor:
    def __init__(self, interval=0.5):
        self.interval = interval
        self.running = False
        self.metrics = []
        self.thread = None

    def start(self):
        self.running = True
        self.thread = threading.Thread(target=self._monitor_loop)
        self.thread.start()

    def stop(self):
        self.running = False
        if self.thread:
            self.thread.join()

    def _monitor_loop(self):
        # Initial call to cpu_percent returns 0, so we discard it or just start loop
        psutil.cpu_percent(interval=None)
        
        while self.running:
            timestamp = time.time()
            cpu = psutil.cpu_percent(interval=None)
            memory = psutil.virtual_memory().percent
            
            # Disk I/O (system wide)
            disk_io = psutil.disk_io_counters()
            read_bytes = disk_io.read_bytes if disk_io else 0
            write_bytes = disk_io.write_bytes if disk_io else 0
            
            self.metrics.append({
                "timestamp": timestamp,
                "cpu_percent": cpu,
                "memory_percent": memory,
                "disk_read_bytes": read_bytes,
                "disk_write_bytes": write_bytes
            })
            time.sleep(self.interval)

class StressTester:
    def __init__(self, host, port, concurrency, total_requests, duration=None):
        self.host = host
        self.port = port
        self.concurrency = concurrency
        self.total_requests = total_requests
        self.duration = duration
        self.results = defaultdict(list)
        self.errors = 0
        self.monitor = SystemMonitor(interval=0.2)
        self.lock = threading.Lock()

    def make_request(self, method, path, body=None):
        start_time = time.time()
        try:
            conn = http.client.HTTPConnection(self.host, self.port, timeout=5)
            headers = {"Content-Type": "application/json"} if body else {}
            
            json_body = json.dumps(body) if body else None
            
            conn.request(method, path, body=json_body, headers=headers)
            resp = conn.getresponse()
            resp.read() # Consume response
            conn.close()
            
            latency = (time.time() - start_time) * 1000 # ms
            return method + " " + path.split('/')[1], latency, resp.status
        except Exception as e:
            # print(f"Error: {e}")
            return "ERROR", 0, 0

    def _generate_random_op(self):
        op_type = random.choice(["read", "write", "update"])
        key = random.randint(1, 50000)
        val = f"value_{random.randint(1, 10000)}"
        
        if op_type == "read":
            return "GET", f"/get_key/{key}", None
        elif op_type == "write":
            return "POST", f"/insert/{key}/{val}", None
        elif op_type == "update":
            return "PUT", f"/update_key/{key}/{val}_updated", None
        elif op_type == "delete":
            return "DELETE", f"/delete_key/{key}", None
        elif op_type == "bulk_write":
            ops = []
            for _ in range(random.randint(1, 5)):
                k = random.randint(1, 1000)
                v = f"val_{k}"
                b_op = random.choice(["insert", "update", "delete"])
                op_entry = {"operation": b_op, "key": k}
                if b_op != "delete":
                    op_entry["value"] = v
                ops.append(op_entry)
            return "POST", "/bulk_update", {"operations": ops}
        elif op_type == "bulk_read":
            keys = [random.randint(1, 1000) for _ in range(random.randint(1, 5))]
            return "PATCH", "/bulk_query", {"data": keys}
        return "GET", f"/get_key/{key}", None

    def _record_result(self, op, latency):
        # Group by operation type
        if "get_key" in op: op_key = "READ"
        elif "insert" in op: op_key = "WRITE"
        elif "update_key" in op: op_key = "UPDATE"
        elif "delete_key" in op: op_key = "DELETE"
        elif "bulk_update" in op: op_key = "BULK_WRITE"
        elif "bulk_query" in op: op_key = "BULK_READ"
        else: op_key = "OTHER"
        
        with self.lock:
            self.results[op_key].append(latency)

    def run_load(self):
        print(f"Starting stress test on {self.host}:{self.port}")
        if self.duration:
            print(f"Concurrency: {self.concurrency}, Duration: {self.duration} minutes")
        else:
            print(f"Concurrency: {self.concurrency}, Total Requests: {self.total_requests}")
        
        self.monitor.start()
        start_total = time.time()
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.concurrency) as executor:
            futures = []
            
            if self.duration:
                end_time = start_total + (self.duration * 60)
                
                def worker():
                    while time.time() < end_time:
                        method, path, body = self._generate_random_op()
                        op, latency, status = self.make_request(method, path, body)
                        if op == "ERROR" or status >= 500:
                            with self.lock:
                                self.errors += 1
                        else:
                            self._record_result(op, latency)

                # Launch worker threads
                worker_futures = [executor.submit(worker) for _ in range(self.concurrency)]
                concurrent.futures.wait(worker_futures)
                
            else:
                for i in range(self.total_requests):
                    method, path, body = self._generate_random_op()
                    futures.append(executor.submit(self.make_request, method, path, body))

                for future in concurrent.futures.as_completed(futures):
                    op, latency, status = future.result()
                    if op == "ERROR" or status >= 500:
                        self.errors += 1
                    else:
                        self._record_result(op, latency)

        self.monitor.stop()
        total_time = time.time() - start_total
        self.print_report(total_time)

    def print_report(self, total_time):
        print("\n" + "="*50)
        print(f"STRESS TEST RESULTS ({total_time:.2f}s)")
        print("="*50)
        print(f"{'Operation':<15} | {'Count':<8} | {'Avg (ms)':<10} | {'P95 (ms)':<10} | {'P99 (ms)':<10}")
        print("-" * 65)
        
        all_latencies = []
        
        for op, latencies in sorted(self.results.items()):
            count = len(latencies)
            avg = statistics.mean(latencies)
            p95 = statistics.quantiles(latencies, n=20)[18] if count > 1 else latencies[0]
            p99 = statistics.quantiles(latencies, n=100)[98] if count > 1 else latencies[0]
            
            all_latencies.extend(latencies)
            
            print(f"{op:<15} | {count:<8} | {avg:<10.2f} | {p95:<10.2f} | {p99:<10.2f}")

        print("-" * 65)
        if all_latencies:
            total_reqs = len(all_latencies)
            rps = total_reqs / total_time
            print(f"Total Requests: {total_reqs}")
            print(f"Errors: {self.errors}")
            print(f"Throughput: {rps:.2f} req/s")
            
            # Save summary for benchmark suite
            with open("last_run_summary.json", "w") as f:
                json.dump({
                    "concurrency": self.concurrency,
                    "throughput": rps,
                    "avg_latency": statistics.mean(all_latencies) if all_latencies else 0
                }, f)
        else:
            print("No successful requests.")
        print("="*50)
        
        # Generate CSV for visualization
        with open("stress_test_results.csv", "w") as f:
            f.write("operation,latency_ms\n")
            for op, latencies in self.results.items():
                for lat in latencies:
                    f.write(f"{op},{lat}\n")
        print("\nDetailed results saved to 'stress_test_results.csv'")

        # Save system metrics
        if self.monitor.metrics:
            with open("system_metrics.csv", "w") as f:
                f.write("timestamp,cpu_percent,memory_percent,disk_read_bytes,disk_write_bytes\n")
                for m in self.monitor.metrics:
                    f.write(f"{m['timestamp']},{m['cpu_percent']},{m['memory_percent']},{m['disk_read_bytes']},{m['disk_write_bytes']}\n")
            print("System metrics saved to 'system_metrics.csv'")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Stress Test for KV Store')
    parser.add_argument('--concurrency', type=int, default=10, help='Number of concurrent threads')
    parser.add_argument('--requests', type=int, default=1000, help='Total number of requests (ignored if duration is set)')
    parser.add_argument('--duration', type=float, help='Duration of test in minutes')
    parser.add_argument('host_url', nargs='?', default="http://localhost:2222", help='Target URL (e.g. http://localhost:2222)')
    
    args = parser.parse_args()
    
    parsed = urlparse(args.host_url)
    host = parsed.hostname or "localhost"
    port = parsed.port or 2222
    
    tester = StressTester(host, port, args.concurrency, args.requests, args.duration)
    
    tester.run_load()
    
    print("\n" + "="*50)
    print("SYSTEM METRICS")
    print("="*50)
    for metric in tester.monitor.metrics:
        print(f"Time: {metric['timestamp']:.2f}s, CPU: {metric['cpu_percent']}%, Memory: {metric['memory_percent']}%, Disk Read: {metric['disk_read_bytes']} bytes, Disk Write: {metric['disk_write_bytes']} bytes")
    print("="*50)
