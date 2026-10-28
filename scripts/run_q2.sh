#!/usr/bin/env bash
# Query 2: 4 executors x (1 core, 2 GB), driver 2 GB.
# {df, sql} x RUNS επαναλήψεις, κάθε εκτέλεση σε νέα εφαρμογή.
# Χρήση:        bash scripts/run_q2.sh
# Προαιρετικά: DROP_CACHES=1 RUNS=5 bash scripts/run_q2.sh
set -euo pipefail
cd ~/advdb
mkdir -p results logs

RUNS=${RUNS:-3}
CONF=(--master spark://localhost:7077 --driver-memory 2g
      --executor-cores 1 --executor-memory 2g --total-executor-cores 4)

rm -f results/q2_times.csv

# Εναλλαγή df/sql σε κάθε γύρο, ώστε η cache του λειτουργικού να επηρεάζει και τα δύο εξίσου
for run in $(seq 1 "$RUNS"); do
  for api in df sql; do
    if [[ "${DROP_CACHES:-0}" == "1" ]]; then
      sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
    fi
    spark-submit "${CONF[@]}" code/q2.py "$api" "$run" 2>> logs/q2_stderr.log
  done
done

python3 code/summarize_times.py results/q2_times.csv
