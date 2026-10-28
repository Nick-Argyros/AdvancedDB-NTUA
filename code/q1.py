"""
Query 1: κριτικές 2015-2021 με >= 1 ψήφο useful, ανά κατηγορία μήκους κειμένου.
  (α) ανά κατηγορία: πλήθος, μέση βαθμολογία, μέσες ψήφοι useful (φθίνουσα κατά useful)
  (β) ανά έτος: ποσοστό (%) των κριτικών του έτους σε κάθε κατηγορία

Χρήση:
  spark-submit ... q1.py <api> <cache> <run> [hold]
    api   : df | sql
    cache : none     -> χωρίς cache, τα (α) και (β) διαβάζουν ξανά το Parquet
            filtered -> cache() του φιλτραρισμένου συνόλου (year, stars, useful, len_cat)
            full     -> cache() ολόκληρου του πίνακα κριτικών (μαζί με το text)
    run   : αριθμός επανάληψης (στο run 1 γράφονται και τα αποτελέσματα / πλάνα)
    hold  : αν δοθεί, η εφαρμογή περιμένει Enter στο τέλος, για να δεις
            την καρτέλα Storage στο http://localhost:4040

Μόνο built-in συναρτήσεις. Το μήκος είναι πλήθος χαρακτήρων: length(text).
"""
import contextlib
import io
import os
import sys
import time

from pyspark.sql import SparkSession, functions as F

api, cache, run = sys.argv[1], sys.argv[2], sys.argv[3]
hold = len(sys.argv) > 4 and sys.argv[4] == "hold"
assert api in ("df", "sql") and cache in ("none", "filtered", "full")

BASE = os.path.expanduser("~/advdb")
PQ = "file://" + os.path.join(BASE, "parquet")
RES = os.path.join(BASE, "results")
os.makedirs(RES, exist_ok=True)

tag = f"q1_{api}_{cache}"
spark = SparkSession.builder.appName(f"{tag}_run{run}").getOrCreate()

# Κατηγορίες μήκους: (ετικέτα, όνομα στήλης για το (β))
CATS = [("Σύντομη (<250)", "short"),
        ("Μεσαία (250-749)", "medium"),
        ("Εκτενής (750-1999)", "long"),
        ("Πολύ εκτενής (>=2000)", "very_long")]

# =====================================================================
# DataFrame API
# =====================================================================
def build_df():
    reviews = spark.read.parquet(f"{PQ}/review")
    if cache == "full":
        reviews = reviews.cache()

    length = F.length("text")
    len_cat = (F.when(length < 250, CATS[0][0])
                .when(length < 750, CATS[1][0])
                .when(length < 2000, CATS[2][0])
                .otherwise(CATS[3][0]))

    filtered = (reviews
                .filter(F.col("year").between(2015, 2021) & (F.col("useful") >= 1))
                .select("year", "stars", "useful", len_cat.alias("len_cat")))
    if cache == "filtered":
        filtered = filtered.cache()

    qa = (filtered.groupBy("len_cat")
          .agg(F.count("*").alias("n_reviews"),
               F.round(F.avg("stars"), 3).alias("avg_stars"),
               F.round(F.avg("useful"), 3).alias("avg_useful"))
          .orderBy(F.desc("avg_useful")))

    qb = (filtered.groupBy("year")
          .agg(F.count("*").alias("n_reviews"),
               *[F.round(F.avg(F.when(F.col("len_cat") == label, 1).otherwise(0)) * 100, 2)
                  .alias(f"pct_{name}") for label, name in CATS])
          .orderBy("year"))
    return qa, qb, filtered, reviews


# =====================================================================
# SQL API
# =====================================================================
def build_sql():
    spark.read.parquet(f"{PQ}/review").createOrReplaceTempView("reviews")
    if cache == "full":
        spark.sql("CACHE LAZY TABLE reviews")

    filtered_sql = f"""
        SELECT year, stars, useful,
               CASE WHEN length(text) < 250  THEN '{CATS[0][0]}'
                    WHEN length(text) < 750  THEN '{CATS[1][0]}'
                    WHEN length(text) < 2000 THEN '{CATS[2][0]}'
                    ELSE '{CATS[3][0]}' END AS len_cat
        FROM reviews
        WHERE year BETWEEN 2015 AND 2021 AND useful >= 1
    """
    if cache == "filtered":
        # LAZY: όπως το df.cache(), υλοποιείται στο πρώτο action (το (α))
        spark.sql(f"CACHE LAZY TABLE q1_filtered AS {filtered_sql}")
    else:
        spark.sql(f"CREATE OR REPLACE TEMP VIEW q1_filtered AS {filtered_sql}")

    qa = spark.sql("""
        SELECT len_cat,
               count(*)                 AS n_reviews,
               round(avg(stars), 3)     AS avg_stars,
               round(avg(useful), 3)    AS avg_useful
        FROM q1_filtered
        GROUP BY len_cat
        ORDER BY avg_useful DESC
    """)

    pct_cols = ",\n".join(
        f"round(avg(CASE WHEN len_cat = '{label}' THEN 1 ELSE 0 END) * 100, 2) AS pct_{name}"
        for label, name in CATS)
    qb = spark.sql(f"""
        SELECT year, count(*) AS n_reviews,
               {pct_cols}
        FROM q1_filtered
        GROUP BY year
        ORDER BY year
    """)
    return qa, qb, spark.table("q1_filtered"), spark.table("reviews")


qa, qb, filtered, reviews = build_df() if api == "df" else build_sql()

# =====================================================================
# Εκτέλεση: (α) και μετά (β), διαδοχικά στην ίδια εφαρμογή
# =====================================================================
t0 = time.perf_counter()
rows_a = qa.collect()
t1 = time.perf_counter()
rows_b = qb.collect()
t2 = time.perf_counter()

with open(os.path.join(RES, "q1_times.csv"), "a") as f:
    f.write(f"{tag}_a,{run},{t1 - t0:.3f}\n")
    f.write(f"{tag}_b,{run},{t2 - t1:.3f}\n")
    f.write(f"{tag}_total,{run},{t2 - t0:.3f}\n")
print(f"[q1] {tag} run={run}  (a)={t1 - t0:.2f}s  (b)={t2 - t1:.2f}s  total={t2 - t0:.2f}s")


def explain_str(df):
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        df.explain(mode="formatted")
    return buf.getvalue()


# Μέγεθος cached συνόλου (ίδια νούμερα με την καρτέλα Storage)
if cache != "none":
    lines = []
    for info in spark.sparkContext._jsc.sc().getRDDStorageInfo():
        lines.append(
            f"{info.name()}\n"
            f"  cached partitions : {info.numCachedPartitions()} / {info.numPartitions()}\n"
            f"  memory size       : {info.memSize() / 2**20:.1f} MiB\n"
            f"  disk size         : {info.diskSize() / 2**20:.1f} MiB\n")
    with open(os.path.join(RES, f"{tag}_storage_run{run}.txt"), "w") as f:
        f.write("\n".join(lines) if lines else "(κανένα cached RDD)\n")
    print("".join(lines))

# Αποτελέσματα και πλάνα μόνο στο run 1 (εκτός χρονομέτρησης)
if run == "1":
    with open(os.path.join(RES, f"{tag}_result.txt"), "w") as f:
        f.write("(α)\n" + "\t".join(qa.columns) + "\n")
        f.writelines("\t".join(str(v) for v in r) + "\n" for r in rows_a)
        f.write("\n(β)\n" + "\t".join(qb.columns) + "\n")
        f.writelines("\t".join(str(v) for v in r) + "\n" for r in rows_b)
    with open(os.path.join(RES, f"{tag}_explain.txt"), "w") as f:
        f.write("########## φιλτραρισμένο σύνολο ##########\n" + explain_str(filtered))
        f.write("\n########## (α) ##########\n" + explain_str(qa))
        f.write("\n########## (β) ##########\n" + explain_str(qb))

if hold:
    input("Η εφαρμογή περιμένει: δες το http://localhost:4040/storage/ και πάτα Enter... ")

spark.catalog.clearCache()
spark.stop()
