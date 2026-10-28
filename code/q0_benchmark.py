"""
Ζητούμενο 0: «μέση βαθμολογία κριτικών ανά έτος» σε τρεις εκδοχές.
  python: spark-submit ... q0_benchmark.py <json_infer|json_schema|parquet> <run>
Ο χρόνος μετριέται από την ανάγνωση (περιλαμβάνει το schema inference)
μέχρι το collect() του τελικού, μικρού αποτελέσματος.
"""
import contextlib
import io
import os
import sys
import time

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (StructType, StructField, StringType,
                               DoubleType, IntegerType)

MODES = ("json_infer", "json_schema", "parquet")
mode = sys.argv[1]
run = sys.argv[2] if len(sys.argv) > 2 else "1"
if mode not in MODES:
    sys.exit(f"mode must be one of {MODES}")

BASE = os.path.expanduser("~/advdb")
RAW = "file://" + os.path.join(BASE, "raw")
PQ = "file://" + os.path.join(BASE, "parquet")
RES = os.path.join(BASE, "results")
os.makedirs(RES, exist_ok=True)

spark = SparkSession.builder.appName(f"q0_{mode}_run{run}").getOrCreate()

REVIEW_SCHEMA = StructType([
    StructField("review_id", StringType()), StructField("user_id", StringType()),
    StructField("business_id", StringType()), StructField("stars", DoubleType()),
    StructField("useful", IntegerType()), StructField("funny", IntegerType()),
    StructField("cool", IntegerType()), StructField("text", StringType()),
    StructField("date", StringType()),
])
REVIEW_JSON = f"{RAW}/yelp_academic_dataset_review.json"
YEAR_FROM_STRING = F.year(F.try_to_timestamp(F.col("date"), F.lit("yyyy-MM-dd HH:mm:ss")))

t0 = time.perf_counter()

if mode == "json_infer":
    df = spark.read.json(REVIEW_JSON)          # διαβάζει ΟΛΟ το αρχείο για να βρει το schema
    year = YEAR_FROM_STRING
elif mode == "json_schema":
    df = spark.read.schema(REVIEW_SCHEMA).json(REVIEW_JSON)
    year = YEAR_FROM_STRING
else:
    df = spark.read.parquet(f"{PQ}/review")
    year = F.col("year")                       # partition column, δεν διαβάζεται από τα αρχεία

q = (df.groupBy(year.alias("year"))
       .agg(F.avg("stars").alias("avg_stars"), F.count("*").alias("n_reviews"))
       .orderBy("year"))
rows = q.collect()

elapsed = time.perf_counter() - t0

with open(os.path.join(RES, "q0_times.csv"), "a") as f:
    f.write(f"{mode},{run},{elapsed:.3f}\n")
print(f"[q0] mode={mode} run={run} time={elapsed:.2f}s")

# Μόνο στην 1η επανάληψη: αποτέλεσμα + πλάνο (εκτός χρονομέτρησης)
if run == "1":
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        q.explain(mode="formatted")
    with open(os.path.join(RES, f"q0_explain_{mode}.txt"), "w") as f:
        f.write(buf.getvalue())
    with open(os.path.join(RES, f"q0_result_{mode}.txt"), "w") as f:
        f.write("year,avg_stars,n_reviews\n")
        for r in rows:
            f.write(f"{r['year']},{r['avg_stars']:.4f},{r['n_reviews']}\n")

spark.stop()
