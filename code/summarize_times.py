"""
Διάμεσος και εύρος χρόνων ανά διαμόρφωση.
Είσοδος: CSV με γραμμές  <config>,<run>,<seconds>
Χρήση:   python3 summarize_times.py results/q0_times.csv
"""
import collections
import csv
import statistics
import sys

times = collections.OrderedDict()
with open(sys.argv[1]) as f:
    for row in csv.reader(f):
        if len(row) < 3:
            continue
        times.setdefault(row[0], []).append(float(row[-1]))

print(f"{'config':<28}{'n':>3}{'median(s)':>12}{'min(s)':>10}{'max(s)':>10}{'range(s)':>10}")
for cfg, v in times.items():
    print(f"{cfg:<28}{len(v):>3}{statistics.median(v):>12.2f}"
          f"{min(v):>10.2f}{max(v):>10.2f}{max(v) - min(v):>10.2f}")
