#!/usr/bin/env bash
# Simple poller that fetches /metrics JSON and prints select fields. Requires jq.
# Usage: ./scripts/poll_metrics.sh http://localhost:2222/metrics 5

URL=${1:-http://localhost:2222/metrics}
INTERVAL=${2:-5}

if ! command -v jq >/dev/null 2>&1; then
  echo "This script requires jq. Install it (apt: jq) and retry." >&2
  exit 2
fi

while true; do
  ts=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
  body=$(curl -sS "$URL")
  if [ $? -ne 0 ] || [ -z "$body" ]; then
    echo "$ts ERROR: failed to fetch $URL"
  else
    cpu=$(echo "$body" | jq -r '.cpu_utilization_percent // 0')
    mem_total=$(echo "$body" | jq -r '.memory_kb.total // 0')
    mem_avail=$(echo "$body" | jq -r '.memory_kb.available // 0')
    disk_rps=$(echo "$body" | jq -r '.disk_read_bytes_per_sec // 0')
    disk_wps=$(echo "$body" | jq -r '.disk_write_bytes_per_sec // 0')
    net_rps=$(echo "$body" | jq -r '.network_rx_bytes_per_sec // 0')
    net_tps=$(echo "$body" | jq -r '.network_tx_bytes_per_sec // 0')
    echo "$ts CPU=${cpu}% MEM_avail=${mem_avail}kB MEM_total=${mem_total}kB DR/s=${disk_rps} DW/s=${disk_wps} NR/s=${net_rps} NW/s=${net_tps}"
  fi
  sleep "$INTERVAL"
done
