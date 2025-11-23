import argparse
import time
import random
import asyncio
import aiohttp
import json
import statistics
import sys
import requests # Keep for populate

# Configuration
HOT_KEY_RANGE = (1, 1000)
COLD_KEY_RANGE = (1001, 500000)

def get_random_string(length=10):
    return "".join(random.choices("abcdefghijklmnopqrstuvwxyz", k=length))

def generate_workload_1():
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
    return op, key, val

def generate_workload_2():
    if random.random() < 0.025:
        op = 'GET'
        key = random.randint(1, 500000)
        val = None
    else:
        op = random.choice(['POST', 'PUT', 'DELETE'])
        val = get_random_string()
        if random.random() < 0.9:
            key = random.randint(*COLD_KEY_RANGE)
        else:
            key = random.randint(*HOT_KEY_RANGE)
            if op == 'POST': 
                op = random.choice(['PUT', 'DELETE'])
    return op, key, val

def generate_workload_3():
    if random.random() < 0.85:
        key = random.randint(*HOT_KEY_RANGE)
    else:
        key = random.randint(*COLD_KEY_RANGE)
    op = random.choice(['GET', 'POST', 'PUT', 'DELETE'])
    val = get_random_string() if op != 'GET' and op != 'DELETE' else None
    return op, key, val

def generate_workload_4():
    # CPU Saturation: GET only, Key range 1-100
    key = random.randint(1, 100)
    op = 'GET'
    val = None
    return op, key, val

def generate_workload_5():
    # Disk Saturation: 100% Writes (PUT/POST) with larger values
    # Use a wider key range to avoid row locking contention if any, 
    # but small enough to hit cache/disk churn if we were reading.
    # Since we are writing, we just want to push bytes.
    key = random.randint(1, 100000)
    op = random.choice(['POST', 'PUT'])
    # Use a larger value (e.g. 1KB) to saturate disk bandwidth
    val = get_random_string(1024) 
    return op, key, val

async def make_request(session, base_url, op, key, val=None):
    url = f"{base_url}"
    start = time.time()
    try:
        if op == 'GET':
            async with session.get(f"{url}/get_key/{key}", timeout=5) as resp:
                status = resp.status
                await resp.read()
        elif op == 'POST':
            async with session.post(f"{url}/insert/{key}/{val}", timeout=5) as resp:
                status = resp.status
                await resp.read()
        elif op == 'PUT':
            async with session.put(f"{url}/update_key/{key}/{val}", timeout=5) as resp:
                status = resp.status
                await resp.read()
        elif op == 'DELETE':
            async with session.delete(f"{url}/delete_key/{key}", timeout=5) as resp:
                status = resp.status
                await resp.read()
        
        latency = time.time() - start
        return status, latency
    except Exception:
        return -1, 0

async def request_task(session, url, workload_type, stats):
    if workload_type == 1:
        op, key, val = generate_workload_1()
    elif workload_type == 2:
        op, key, val = generate_workload_2()
    elif workload_type == 3:
        op, key, val = generate_workload_3()
    elif workload_type == 4:
        op, key, val = generate_workload_4()
    elif workload_type == 5:
        op, key, val = generate_workload_5()
    else:
        return

    code, lat = await make_request(session, url, op, key, val)

    if code >= 200 and code < 300:
        stats['success'] += 1
        stats['latencies'].append(lat)
    else:
        stats['fail'] += 1

async def run_test(args):
    stats = {'success': 0, 'fail': 0, 'latencies': []}
    
    # Increase connection limit for high concurrency
    connector = aiohttp.TCPConnector(limit=0, ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=connector) as session:
        start_time = time.time()
        end_time = start_time + args.duration
        expected_time = start_time
        
        tasks = []
        
        while True:
            now = time.time()
            if now >= end_time:
                break
                
            inter_arrival = random.expovariate(args.rate)
            expected_time += inter_arrival
            
            wait_time = expected_time - now
            if wait_time > 0:
                await asyncio.sleep(wait_time)
            
            task = asyncio.create_task(request_task(session, args.url, args.workload, stats))
            tasks.append(task)
            
        # Wait for pending tasks (with a timeout to avoid hanging forever if server is dead)
        if tasks:
            await asyncio.wait(tasks, timeout=10)
            
    return stats

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
    parser = argparse.ArgumentParser(description='Open Loop Load Generator')
    parser.add_argument('--url', type=str, required=True, help='Server URL')
    parser.add_argument('--workload', type=int, required=True, choices=[1, 2, 3, 4, 5], help='Workload type (1, 2, 3, 4, 5)')
    parser.add_argument('--rate', type=float, required=True, help='Arrival rate (requests/second)')
    parser.add_argument('--duration', type=int, default=60, help='Duration of test in seconds')
    parser.add_argument('--populate', action='store_true', help='Populate hot keys before running')
    
    args = parser.parse_args()
    
    if args.populate:
        populate_hot_keys(args.url)
        
    print(f"Starting Open Loop Workload {args.workload} with rate {args.rate} req/s for {args.duration}s...")
    
    try:
        stats = asyncio.run(run_test(args))
    except KeyboardInterrupt:
        pass
    
    # Aggregate results
    total_ops = stats['success'] + stats['fail']
    throughput = stats['success'] / args.duration # Throughput is successful ops over duration
    
    if stats['latencies']:
        avg_latency = statistics.mean(stats['latencies'])
        p50_latency = statistics.median(stats['latencies'])
        p95_latency = sorted(stats['latencies'])[int(len(stats['latencies']) * 0.95)]
        p99_latency = sorted(stats['latencies'])[int(len(stats['latencies']) * 0.99)]
    else:
        avg_latency = 0
        p50_latency = 0
        p95_latency = 0
        p99_latency = 0
        
    result = {
        'workload': args.workload,
        'rate': args.rate,
        'duration': args.duration,
        'throughput': throughput,
        'avg_latency': avg_latency,
        'p50_latency': p50_latency,
        'p95_latency': p95_latency,
        'p99_latency': p99_latency,
        'success': stats['success'],
        'fail': stats['fail']
    }
    
    print(json.dumps(result, indent=2))

if __name__ == "__main__":
    main()
