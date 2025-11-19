import csv
import json
from collections import defaultdict

def generate_html_report(latency_csv, system_csv, output_file):
    latency_data = defaultdict(list)
    with open(latency_csv, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            latency_data[row['operation']].append(float(row['latency_ms']))

    labels = list(latency_data.keys())
    avg_data = []
    p95_data = []
    
    for op in labels:
        latencies = sorted(latency_data[op])
        avg = sum(latencies) / len(latencies)
        p95_idx = int(len(latencies) * 0.95)
        p95 = latencies[p95_idx]
        avg_data.append(avg)
        p95_data.append(p95)

    timestamps = []
    cpu_data = []
    mem_data = []
    disk_read_rate = []
    disk_write_rate = []
    
    try:
        with open(system_csv, 'r') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            
            if rows:
                start_time = float(rows[0]['timestamp'])
                prev_read = float(rows[0]['disk_read_bytes'])
                prev_write = float(rows[0]['disk_write_bytes'])
                prev_ts = float(rows[0]['timestamp'])

                for row in rows:
                    curr_ts = float(row['timestamp'])
                    timestamps.append(round(curr_ts - start_time, 1)) # Relative time
                    cpu_data.append(float(row['cpu_percent']))
                    mem_data.append(float(row['memory_percent']))
                    
                    # Calculate rate (MB/s)
                    time_diff = curr_ts - prev_ts
                    curr_read = float(row['disk_read_bytes'])
                    curr_write = float(row['disk_write_bytes'])
                    
                    if time_diff > 0:
                        r_rate = (curr_read - prev_read) / time_diff / (1024*1024)
                        w_rate = (curr_write - prev_write) / time_diff / (1024*1024)
                    else:
                        r_rate = 0
                        w_rate = 0
                    
                    disk_read_rate.append(r_rate)
                    disk_write_rate.append(w_rate)
                    
                    prev_read = curr_read
                    prev_write = curr_write
                    prev_ts = curr_ts
    except FileNotFoundError:
        print("System metrics file not found. Skipping system charts.")

    html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <title>Stress Test & System Metrics</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        body {{ font-family: sans-serif; padding: 20px; background-color: #f4f4f9; }}
        .container {{ width: 90%; margin: auto; background: white; padding: 20px; box-shadow: 0 0 10px rgba(0,0,0,0.1); }}
        h1, h2 {{ text-align: center; color: #333; }}
        .chart-container {{ position: relative; height: 40vh; width: 100%; margin-bottom: 50px; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>System Performance Report</h1>
        
        <h2>API Latency</h2>
        <div class="chart-container">
            <canvas id="latencyChart"></canvas>
        </div>

        <h2>System Resources (CPU & Memory)</h2>
        <div class="chart-container">
            <canvas id="resourceChart"></canvas>
        </div>

        <h2>Disk I/O Throughput</h2>
        <div class="chart-container">
            <canvas id="diskChart"></canvas>
        </div>
    </div>

    <script>
        // --- Latency Chart ---
        new Chart(document.getElementById('latencyChart'), {{
            type: 'bar',
            data: {{
                labels: {json.dumps(labels)},
                datasets: [
                    {{
                        label: 'Avg Latency (ms)',
                        data: {json.dumps(avg_data)},
                        backgroundColor: 'rgba(54, 162, 235, 0.6)'
                    }},
                    {{
                        label: 'P95 Latency (ms)',
                        data: {json.dumps(p95_data)},
                        backgroundColor: 'rgba(255, 99, 132, 0.6)'
                    }}
                ]
            }},
            options: {{ responsive: true, maintainAspectRatio: false }}
        }});

        // --- Resource Chart ---
        new Chart(document.getElementById('resourceChart'), {{
            type: 'line',
            data: {{
                labels: {json.dumps(timestamps)},
                datasets: [
                    {{
                        label: 'CPU Usage (%)',
                        data: {json.dumps(cpu_data)},
                        borderColor: 'rgba(255, 159, 64, 1)',
                        backgroundColor: 'rgba(255, 159, 64, 0.2)',
                        fill: true,
                        tension: 0.4
                    }},
                    {{
                        label: 'Memory Usage (%)',
                        data: {json.dumps(mem_data)},
                        borderColor: 'rgba(75, 192, 192, 1)',
                        backgroundColor: 'rgba(75, 192, 192, 0.2)',
                        fill: true,
                        tension: 0.4
                    }}
                ]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                scales: {{
                    x: {{ title: {{ display: true, text: 'Time (s)' }} }},
                    y: {{ beginAtZero: true, max: 100 }}
                }}
            }}
        }});

        // --- Disk I/O Chart ---
        new Chart(document.getElementById('diskChart'), {{
            type: 'line',
            data: {{
                labels: {json.dumps(timestamps)},
                datasets: [
                    {{
                        label: 'Read (MB/s)',
                        data: {json.dumps(disk_read_rate)},
                        borderColor: 'rgba(153, 102, 255, 1)',
                        backgroundColor: 'rgba(153, 102, 255, 0.2)',
                        fill: true
                    }},
                    {{
                        label: 'Write (MB/s)',
                        data: {json.dumps(disk_write_rate)},
                        borderColor: 'rgba(255, 205, 86, 1)',
                        backgroundColor: 'rgba(255, 205, 86, 0.2)',
                        fill: true
                    }}
                ]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                scales: {{
                    x: {{ title: {{ display: true, text: 'Time (s)' }} }},
                    y: {{ beginAtZero: true, title: {{ display: true, text: 'MB/s' }} }}
                }}
            }}
        }});
    </script>
</body>
</html>
    """

    with open(output_file, 'w') as f:
        f.write(html_content)
    print(f"Report generated: {output_file}")

if __name__ == "__main__":
    generate_html_report("stress_test_results.csv", "system_metrics.csv", "stress_test_report.html")
