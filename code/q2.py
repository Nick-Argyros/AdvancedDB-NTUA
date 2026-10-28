"""
Query 2: για κάθε πολιτεία με >= 1.000 επιχειρήσεις, οι 3 κατηγορίες με την
υψηλότερη μέση βαθμολογία κριτικών (μόνο κατηγορίες με >= 500 κριτικές στην πολιτεία).
Για κάθε κατηγορία: πλήθος κριτικών, μέση βαθμολογία, % κριτικών 5 αστέρων.
Ταξινόμηση ανά πολιτεία, φθίνουσα μέση βαθμολογία· σε ισοβαθμία προηγείται η
κατηγορία με τις περισσότερες κριτικές (και, για ντετερμινισμό, αλφαβητικά).

Μια κριτική μετρά σε ΟΛΕΣ τις κατηγορίες της επιχείρησής της (Tip 1).

Χρήση:
  spark-submit ... q2.py <df|sql> <run>
"""
import contextlib
import io
import os
import sys
import time

from pyspark.sql import SparkSession, Window, functions as F

api, run = sys.argv[1], sys.argv[2]
assert api in ("df", "sql")

BASE = os.path.expanduser("~/advdb")
PQ = "file://" + os.path.join(BASE, "parquet")
RES = os.path.join(BASE, "results")
os.makedirs(RES, exist_ok=True)

MIN_BUSINESSES = 1000
MIN_REVIEWS = 500
TOP_K = 3

tag = f"q2_{api}"
spark = SparkSession.builder.appName(f"{tag}_run{run}").getOrCreate()

business = spark.read.parquet(f"{PQ}/business")
review = spark.read.parquet(f"{PQ}/review")


# =====================================================================
# DataFrame API
# =====================================================================
def build_df():
    big_states = (business.groupBy("state").count()
                  .filter(F.col("count") >= MIN_BUSINESSES)
                  .select("state"))

    biz = (business.select("business_id", "state", "categories")
           .join(big_states, "state", "left_semi"))

    per_cat = (review.select("business_id", "stars")
               .join(biz, "business_id")
               .withColumn("category", F.explode(F.split(F.col("categories"), ", ")))
               .withColumn("category", F.trim("category"))
               .groupBy("state", "category")
               .agg(F.count("*").alias("n_reviews"),
                    F.avg("stars").alias("avg_stars"),
                    (F.avg(F.when(F.col("stars") == 5, 1).otherwise(0)) * 100).alias("pct_5star"))
               .filter(F.col("n_reviews") >= MIN_REVIEWS))

    w = Window.partitionBy("state").orderBy(F.desc("avg_stars"), F.desc("n_reviews"), "category")

    return (per_cat
            .withColumn("rank", F.row_number().over(w))
            .filter(F.col("rank") <= TOP_K)
            .select("state", "rank", "category", "n_reviews",
                    F.round("avg_stars", 2).alias("avg_stars"),
                    F.round("pct_5star", 1).alias("pct_5star"))
            .orderBy("state", "rank"))


# =====================================================================
# SQL API (top-3 με row_number() σε υποερώτημα)
# =====================================================================
def build_sql():
    business.createOrReplaceTempView("business")
    review.createOrReplaceTempView("review")
    return spark.sql(f"""
        WITH big_states AS (
            SELECT state
            FROM business
            GROUP BY state
            HAVING count(*) >= {MIN_BUSINESSES}
        ),
        biz AS (
            SELECT b.business_id, b.state, b.categories
            FROM business b
            LEFT SEMI JOIN big_states s ON b.state = s.state
        ),
        review_cat AS (
            SELECT b.state, trim(c.category) AS category, r.stars
            FROM review r
            JOIN biz b ON r.business_id = b.business_id
            LATERAL VIEW explode(split(b.categories, ', ')) c AS category
        ),
        per_cat AS (
            SELECT state, category,
                   count(*)                                           AS n_reviews,
                   avg(stars)                                         AS avg_stars,
                   avg(CASE WHEN stars = 5 THEN 1 ELSE 0 END) * 100   AS pct_5star
            FROM review_cat
            GROUP BY state, category
            HAVING count(*) >= {MIN_REVIEWS}
        )
        SELECT state, rank, category, n_reviews,
               round(avg_stars, 2) AS avg_stars,
               round(pct_5star, 1) AS pct_5star
        FROM (
            SELECT *,
                   row_number() OVER (PARTITION BY state
                                      ORDER BY avg_stars DESC, n_reviews DESC, category) AS rank
            FROM per_cat
        ) ranked
        WHERE rank <= {TOP_K}
        ORDER BY state, rank
    """)


q = build_df() if api == "df" else build_sql()

t0 = time.perf_counter()
rows = q.collect()
elapsed = time.perf_counter() - t0

with open(os.path.join(RES, "q2_times.csv"), "a") as f:
    f.write(f"{tag},{run},{elapsed:.3f}\n")
print(f"[q2] {tag} run={run} time={elapsed:.2f}s  ({len(rows)} γραμμές)")

# Αποτέλεσμα και πλάνο μόνο στο run 1 (εκτός χρονομέτρησης)
if run == "1":
    with open(os.path.join(RES, f"{tag}_result.txt"), "w") as f:
        f.write("\t".join(q.columns) + "\n")
        f.writelines("\t".join(str(v) for v in r) + "\n" for r in rows)
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        q.explain(mode="formatted")
    with open(os.path.join(RES, f"{tag}_explain.txt"), "w") as f:
        f.write(buf.getvalue())

spark.stop()
