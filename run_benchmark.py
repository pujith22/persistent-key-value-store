import subprocess
import json
import csv
import matplotlib.pyplot as plt
import sys
import time
import os

# Concurrency levels to test
CONCURRENCY_LEVELS = [1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21, 23, 25, 27, 29]
DURATION_MINUTES = 0.2
HOST_URL = "http://localhost:2222"

results = []

print(f"Starting benchmark suite on {HOST_URL}")
print(f"Concurrency levels: {CONCURRENCY_LEVELS}")
print(f"Duration per test: {DURATION_MINUTES} minutes")
print("-" * 50)

for c in CONCURRENCY_LEVELS:
    print(f"Running test with concurrency {c}...")
    
    # Running stress_test.py
    cmd = [
        sys.executable, "stress_test.py",
        "--concurrency", str(c),
        "--duration", str(DURATION_MINUTES),
        HOST_URL
    ]
    
    try:
        subprocess.run(cmd, check=True)
        
        # Read summary from the json file
        if os.path.exists("last_run_summary.json"):
            with open("last_run_summary.json", "r") as f:
                summary = json.load(f)
                results.append(summary)
                print(f"  -> Throughput: {summary['throughput']:.2f} req/s, Latency: {summary['avg_latency']:.2f} ms")
        else:
            print("  -> Error: No summary file found.")
            
    except subprocess.CalledProcessError as e:
        print(f"  -> Error running test: {e}")
    
    # Cool down
    time.sleep(100)

# Save results to CSV
with open("benchmark_results.csv", "w", newline='') as f:
    writer = csv.writer(f)
    writer.writerow(["concurrency", "throughput", "avg_latency"])
    for r in results:
        writer.writerow([r["concurrency"], r["throughput"], r["avg_latency"]])

print("-" * 50)
print("Benchmark complete. Generating plots...")

# Plotting the results
concurrencies = [r["concurrency"] for r in results]
throughputs = [r["throughput"] for r in results]
latencies = [r["avg_latency"] for r in results]

fig, ax1 = plt.subplots(figsize=(10, 6))

color = 'tab:blue'
ax1.set_xlabel('Concurrency (Users)')
ax1.set_ylabel('Throughput (req/s)', color=color)
ax1.plot(concurrencies, throughputs, marker='o', color=color, linewidth=2, label='Throughput')
ax1.tick_params(axis='y', labelcolor=color)
ax1.grid(True, linestyle='--', alpha=0.7)

ax2 = ax1.twinx() # for twin x axis in same plot

color = 'tab:red'
ax2.set_ylabel('Avg Latency (ms)', color=color)
ax2.plot(concurrencies, latencies, marker='s', color=color, linewidth=2, linestyle='--', label='Latency')
ax2.tick_params(axis='y', labelcolor=color)

plt.title('Throughput and Response Time vs Concurrency')
fig.tight_layout()

plt.savefig('benchmark_report.png')
print("Plots saved to 'benchmark_report.png'")
