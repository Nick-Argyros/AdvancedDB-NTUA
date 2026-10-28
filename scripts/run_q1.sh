#!/usr/bin/env bash
# Query 1: 4 executors x (1 core, 2 GB), driver 2 GB.
#   - {df, sql} x {χωρίς cache, cache φιλτραρισμένου}: RUNS επαναλήψεις, νέα εφαρμογή κάθε φορά
#   - cache ολόκληρου του πίνακα (μόνο df): RUNS_FULL επαναλήψεις
# Χρήση:        bash scripts/run_q1.sh
# Προαιρετικά: DROP_CACHES=1 RUNS=3 RUNS_FULL=1 bash scripts/run_q1.sh
set -euo pipefail
cd ~/advdb
mkdir -p results logs

RUNS=${RUNS:-3}
RUNS_FULL=${RUNS_FULL:-1}
CONF=(--master spark://localhost:7077 --driver-memory 2g
      --executor-cores 1 --executor-memory 2g --total-executor-cores 4
      # καταγραφή των cached blocks στα event logs -> καρτέλα Storage και στο History Server
      --conf spark.eventLog.logBlockUpdates.enabled=true)

drop() {
  if [[ "${DROP_CACHES:-0}" == "1" ]]; then
    sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
  fi
}

rm -f results/q1_times.csv

for run in $(seq 1 "$RUNS"); do
  for api in df sql; do
    for cache in none filtered; do
      drop
      spark-submit "${CONF[@]}" code/q1.py "$api" "$cache" "$run" 2>> logs/q1_stderr.log
    done
  done
done

for run in $(seq 1 "$RUNS_FULL"); do
  drop
  spark-submit "${CONF[@]}" code/q1.py df full "$run" 2>> logs/q1_stderr.log
done

python3 code/summarize_times.py results/q1_times.csv
