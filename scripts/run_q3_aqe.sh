#!/usr/bin/env bash
# Query 3: η καλύτερη υλοποίηση με AQE ενεργό και ανενεργό.
# Χρήση:  BEST=merge bash scripts/run_q3_aqe.sh      (BEST = none|broadcast|merge|shuffle_hash)
set -uo pipefail
cd ~/advdb
mkdir -p results logs

BEST=${BEST:?Όρισε BEST=none|broadcast|merge|shuffle_hash}
RUNS=${RUNS:-3}
BASE_CONF=(--master spark://localhost:7077 --driver-memory 2g
           --executor-cores 1 --executor-memory 2g --total-executor-cores 4)

rm -f results/q3_aqe_times.csv

for run in $(seq 1 "$RUNS"); do
  for aqe in on off; do
    if [[ "${DROP_CACHES:-0}" == "1" ]]; then
      sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
    fi
    spark-submit "${BASE_CONF[@]}" code/q3.py df "$BEST" "$run" "$aqe" 2>> logs/q3_stderr.log \
      || echo "[q3] FAILED: df $BEST aqe=$aqe run=$run"
  done
done

# Οι χρόνοι γράφονται στο ίδιο q3_times.csv· κρατάμε μόνο τις γραμμές της BEST
grep "^q3_df_${BEST}_" results/q3_times.csv | tail -n $((2 * RUNS)) > results/q3_aqe_times.csv
python3 code/summarize_times.py results/q3_aqe_times.csv
