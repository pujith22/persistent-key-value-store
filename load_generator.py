import argparse
import time
import random
import requests
import threading
import statistics
import sys
import json

# Configuration
HOT_KEY_RANGE = (1, 1000)
COLD_KEY_RANGE = (1001, 500000)

def get_random_string(length=10):
    return "".join(random.choices("abcdefghijklmnopqrstuvwxyz", k=length))

def make_request(session, base_url, op, key, val=None):
    url = f"{base_url}"
    try:
        if op == 'GET':
            resp = session.get(f"{url}/get_key/{key}", timeout=5)
        elif op == 'POST':
            resp = session.post(f"{url}/insert/{key}/{val}", timeout=5)
        elif op == 'PUT':
            resp = session.put(f"{url}/update_key/{key}/{val}", timeout=5)
        elif op == 'DELETE':
            resp = session.delete(f"{url}/delete_key/{key}", timeout=5)
        return resp.status_code, resp.elapsed.total_seconds()
    except Exception as e:
        return -1, 0

def workload_1(session, base_url):
    # CPU Bottleneck / Hot Keys
    # 85% on Hot Keys (1-1000)
    #   Of these, 95% GET
    # 15% on Cold Keys (1001-500000)
    
    if random.random() < 0.85:
        key = random.randint(*HOT_KEY_RANGE)
        if random.random() < 0.95:
            op = 'GET'
            val = None
        else:
            op = random.choice(['POST', 'PUT', 'DELETE'])
            val = get_random_string()
    else:
        key = random.randint(*COLD_KEY_RANGE)
        op = random.choice(['GET', 'POST', 'PUT', 'DELETE'])
        val = get_random_string()
        
    return make_request(session, base_url, op, key, val)

def workload_2(session, base_url):
    # I/O Bottleneck
    # 2.5% GET
    # 97.5% Write (POST/PUT/DELETE)
    #   Most load on 1001-500000
    #   Some delete/update on 1-1000
    
    if random.random() < 0.025:
        op = 'GET'
        # Random key from full range
        key = random.randint(1, 500000)
        val = None
    else:
        # Write operation
        op = random.choice(['POST', 'PUT', 'DELETE'])
        val = get_random_string()
        
        # "Most of the load on key-space 1001-500000"
        # "Also maybe on delete and update on key-space 1-1000"
        if random.random() < 0.9: # 90% of writes on cold keys
            key = random.randint(*COLD_KEY_RANGE)
        else:
            key = random.randint(*HOT_KEY_RANGE)
            # If on hot key, prefer update/delete as per description? 
            # "delete and update on key-space 1-1000"
            if op == 'POST': 
                op = random.choice(['PUT', 'DELETE'])

    return make_request(session, base_url, op, key, val)

def workload_3(session, base_url):
    # Generic
    # 85% on Hot Keys (1-1000)
    # 15% on Cold Keys
    # Mixed request types (not biased)
    
    if random.random() < 0.85:
        key = random.randint(*HOT_KEY_RANGE)
    else:
        key = random.randint(*COLD_KEY_RANGE)
        
    op = random.choice(['GET', 'POST', 'PUT', 'DELETE'])
    val = get_random_string() if op != 'GET' and op != 'DELETE' else None
    
    return make_request(session, base_url, op, key, val)

def worker(worker_id, url, workload_type, duration, stats, start_barrier, think_time):
    session = requests.Session()
    
    # Wait for all threads to be ready
    start_barrier.wait()
    
    start_time = time.time()
    end_time = start_time + duration
    
    local_latencies = []
    local_success = 0
    local_fail = 0
    
    while time.time() < end_time:
        if workload_type == 1:
            code, lat = workload_1(session, url)
        elif workload_type == 2:
            code, lat = workload_2(session, url)
        elif workload_type == 3:
            code, lat = workload_3(session, url)
        else:
            break
            
        if code >= 200 and code < 300:
            local_success += 1
            local_latencies.append(lat)
        else:
            local_fail += 1
        
        # Adaptive request rate (Think Time)
        # Simulates real-world user behavior and allows for better control over load
        if think_time > 0:
            time.sleep(random.expovariate(1.0 / think_time))
            
    stats[worker_id] = {
        'latencies': local_latencies,
        'success': local_success,
        'fail': local_fail
    }

def populate_hot_keys(url):
    print("Populating hot keys (1-1000)...")
    session = requests.Session()
    for i in range(1, 1001):
        try:
            session.post(f"{url}/insert/{i}/initial_value_{i}")
        except:
            pass
    print("Hot keys populated.")

def main():
    parser = argparse.ArgumentParser(description='Load Generator')
    parser.add_argument('--url', type=str, required=True, help='Server URL')
    parser.add_argument('--workload', type=int, required=True, choices=[1, 2, 3], help='Workload type (1, 2, 3)')
    parser.add_argument('--concurrency', type=int, default=1, help='Number of concurrent threads')
    parser.add_argument('--duration', type=int, default=60, help='Duration of test in seconds')
    parser.add_argument('--populate', action='store_true', help='Populate hot keys before running')
    parser.add_argument('--think-time', type=float, default=0, help='Mean think time in seconds (0 for none)')
    
    args = parser.parse_args()
    
    if args.populate:
        populate_hot_keys(args.url)
        
    print(f"Starting Workload {args.workload} with concurrency {args.concurrency} for {args.duration}s (Think Time: {args.think_time}s)...")
    
    stats = [None] * args.concurrency
    barrier = threading.Barrier(args.concurrency)
    threads = []
    
    for i in range(args.concurrency):
        t = threading.Thread(target=worker, args=(i, args.url, args.workload, args.duration, stats, barrier, args.think_time))
        threads.append(t)
        t.start()
        
    for t in threads:
        t.join()
        
    # Aggregate results
    total_success = sum(s['success'] for s in stats)
    total_fail = sum(s['fail'] for s in stats)
    all_latencies = []
    for s in stats:
        all_latencies.extend(s['latencies'])
        
    throughput = total_success / args.duration
    
    if all_latencies:
        avg_latency = statistics.mean(all_latencies)
        p50_latency = statistics.median(all_latencies)
        p95_latency = sorted(all_latencies)[int(len(all_latencies) * 0.95)]
        p99_latency = sorted(all_latencies)[int(len(all_latencies) * 0.99)]
    else:
        avg_latency = 0
        p50_latency = 0
        p95_latency = 0
        p99_latency = 0
        
    result = {
        'workload': args.workload,
        'concurrency': args.concurrency,
        'duration': args.duration,
        'throughput': throughput,
        'avg_latency': avg_latency,
        'p50_latency': p50_latency,
        'p95_latency': p95_latency,
        'p99_latency': p99_latency,
        'success': total_success,
        'fail': total_fail
    }
    
    print(json.dumps(result, indent=2))

if __name__ == "__main__":
    main()
