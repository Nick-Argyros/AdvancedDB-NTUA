#!/usr/bin/env bash
# Query 3: 4 executors x (1 core, 2 GB), driver 2 GB, AQE ενεργό (προεπιλογή).
#   - df/sql χωρίς hint, και df με BROADCAST / MERGE / SHUFFLE_HASH: RUNS επαναλήψεις
#   - SHUFFLE_REPLICATE_NL: μόνο explain + μία εκτέλεση σε δείγμα 0,1%
# Μια αποτυχία (π.χ. OOM) καταγράφεται ως FAILED και το script συνεχίζει.
# Χρήση:        bash scripts/run_q3.sh
# Προαιρετικά: DROP_CACHES=1 RUNS=3 bash scripts/run_q3.sh
set -uo pipefail
cd ~/advdb
mkdir -p results logs

RUNS=${RUNS:-3}
CONF=(--master spark://localhost:7077 --driver-memory 2g
      --executor-cores 1 --executor-memory 2g --total-executor-cores 4)

drop() {
  if [[ "${DROP_CACHES:-0}" == "1" ]]; then
    sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
  fi
}

submit() {   # submit <api> <hint> <run>
  drop
  if ! spark-submit "${CONF[@]}" code/q3.py "$@" 2>> logs/q3_stderr.log; then
    echo "[q3] FAILED: $*" | tee -a results/q3_failures.txt
  fi
}

rm -f results/q3_times.csv results/q3_failures.txt

for run in $(seq 1 "$RUNS"); do
  submit df  none         "$run"
  submit sql none         "$run"
  submit df  broadcast    "$run"
  submit df  merge        "$run"
  submit df  shuffle_hash "$run"
done

submit df  shuffle_replicate_nl 1
submit sql shuffle_replicate_nl 1
submit df  snl_sample           1

python3 code/summarize_times.py results/q3_times.csv
[[ -f results/q3_failures.txt ]] && cat results/q3_failures.txt
exit 0
