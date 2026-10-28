#!/usr/bin/env bash
# Ζητούμενο 0: 3 επαναλήψεις x 3 εκδοχές, κάθε εκτέλεση σε νέα εφαρμογή.
# Χρήση:   bash scripts/run_q0.sh
# Προαιρετικά: DROP_CACHES=1 bash scripts/run_q0.sh   (άδειασμα page cache πριν από κάθε run)
set -euo pipefail
cd ~/advdb
mkdir -p results logs

MASTER=spark://localhost:7077
RUNS=${RUNS:-3}
CONF=(--master "$MASTER" --driver-memory 2g
      --executor-cores 1 --executor-memory 2g --total-executor-cores 4)

rm -f results/q0_times.csv

# Οι εκδοχές εναλλάσσονται σε κάθε γύρο, ώστε η επίδραση της cache του
# λειτουργικού να μοιράζεται δίκαια.
for run in $(seq 1 "$RUNS"); do
  for mode in json_infer json_schema parquet; do
    if [[ "${DROP_CACHES:-0}" == "1" ]]; then
      sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
    fi
    spark-submit "${CONF[@]}" code/q0_benchmark.py "$mode" "$run" 2>> logs/q0_stderr.log
  done
done

python3 code/summarize_times.py results/q0_times.csv
