"""
Query 3: ανά έτος εγγραφής χρηστών (year(yelping_since)):
  (α) πλήθος κριτικών των χρηστών αυτών
  (β) μέση βαθμολογία των κριτικών τους
  (γ) μέση «αυστηρότητα» = avg(review.stars - business.stars)
  (δ) % κριτικών γραμμένων σε έτος όπου ο συντάκτης ήταν elite
  (ε) πλήθος χρηστών του έτους χωρίς καμία κριτική στο review.json
Ταξινόμηση κατά έτος εγγραφής, αύξουσα.

Χρήση:
  spark-submit ... q3.py <api> <hint> <run> [aqe]
    api  : df | sql
    hint : none | broadcast | merge | shuffle_hash   -> hint στο review ⋈ user (εκτελείται)
           shuffle_replicate_nl                      -> ΜΟΝΟ explain, χωρίς εκτέλεση
           snl_sample                                -> SHUFFLE_REPLICATE_NL σε δείγμα 0,1%
    run  : αριθμός επανάληψης (στο run 1 γράφονται αποτέλεσμα και πλάνο)
    aqe  : on (προεπιλογή) | off  -> spark.sql.adaptive.enabled
"""
import contextlib
import io
import os
import sys
import time

from pyspark.sql import SparkSession, functions as F

api, hint, run = sys.argv[1], sys.argv[2], sys.argv[3]
aqe_arg = sys.argv[4] if len(sys.argv) > 4 else "on"
assert aqe_arg in ("on", "off")
HINTS = ("none", "broadcast", "merge", "shuffle_hash", "shuffle_replicate_nl", "snl_sample")
assert api in ("df", "sql") and hint in HINTS

BASE = os.path.expanduser("~/advdb")
PQ = "file://" + os.path.join(BASE, "parquet")
RES = os.path.join(BASE, "results")
os.makedirs(RES, exist_ok=True)

aqe = "aqe" if aqe_arg == "on" else "noaqe"
tag = f"q3_{api}_{hint}_{aqe}"
spark = (SparkSession.builder.appName(f"{tag}_run{run}")
         .config("spark.sql.adaptive.enabled", "true" if aqe_arg == "on" else "false")
         .getOrCreate())

join_hint = "shuffle_replicate_nl" if hint == "snl_sample" else hint

# ---------------------------------------------------------------------
# Πεδίο elite: έτη χωρισμένα με κόμμα. Ανωμαλία (Tip 3): το 2020 εμφανίζεται
# και ως "20,20". Διορθώνουμε ΜΟΝΟ όταν τα "20" είναι ολόκληρα στοιχεία της λίστας
# (το "2020,2021" περιέχει επίσης τη συμβολοσειρά "20,20" και δεν πρέπει να αλλάξει),
# και μετά κρατάμε μόνο 4ψήφια έτη.
# ---------------------------------------------------------------------
ELITE_FIXED = r"regexp_replace(coalesce(elite, ''), '(^|,)20,20(,|$)', '$12020$2')"
ELITE_YEARS = (f"transform(filter(split({ELITE_FIXED}, ','), x -> x rlike '^[0-9]{{4}}$'), "
               f"x -> cast(x AS INT))")

user_raw = spark.read.parquet(f"{PQ}/user")
review_raw = spark.read.parquet(f"{PQ}/review")
business_raw = spark.read.parquet(f"{PQ}/business")

if hint == "snl_sample":
    # Δείγμα 0,1% και των δύο πλευρών: τα αποτελέσματα δεν έχουν νόημα,
    # μετράμε μόνο το κόστος του nested loop.
    user_raw = user_raw.sample(fraction=0.001, seed=42)
    review_raw = review_raw.sample(fraction=0.001, seed=42)


# =====================================================================
# DataFrame API
# =====================================================================
def build_df():
    users = user_raw.select("user_id",
                            F.year("yelping_since").alias("reg_year"),
                            F.expr(ELITE_YEARS).alias("elite_years"))
    reviews = review_raw.select("user_id", "business_id", "stars", "year")
    biz = business_raw.select("business_id", F.col("stars").alias("biz_stars"))

    users_for_join = users if join_hint == "none" else users.hint(join_hint)

    stats = (reviews.join(users_for_join, "user_id")
             .join(biz, "business_id")
             .groupBy("reg_year")
             .agg(F.count("*").alias("n_reviews"),
                  F.avg("stars").alias("avg_stars"),
                  F.avg(F.col("stars") - F.col("biz_stars")).alias("avg_strictness"),
                  (F.avg(F.when(F.array_contains("elite_years", F.col("year")), 1).otherwise(0))
                   * 100).alias("pct_elite")))

    no_reviews = (users.join(reviews.select("user_id"), "user_id", "left_anti")
                  .groupBy("reg_year")
                  .agg(F.count("*").alias("users_without_reviews")))

    return (stats.join(no_reviews, "reg_year", "full_outer")
            .select("reg_year",
                    F.coalesce("n_reviews", F.lit(0)).alias("n_reviews"),
                    F.round("avg_stars", 3).alias("avg_stars"),
                    F.round("avg_strictness", 3).alias("avg_strictness"),
                    F.round("pct_elite", 2).alias("pct_elite"),
                    F.coalesce("users_without_reviews", F.lit(0)).alias("users_without_reviews"))
            .orderBy("reg_year"))


# =====================================================================
# SQL API
# =====================================================================
def build_sql():
    user_raw.createOrReplaceTempView("users")
    review_raw.createOrReplaceTempView("review")
    business_raw.createOrReplaceTempView("business")
    hint_sql = "" if join_hint == "none" else f"/*+ {join_hint.upper()}(u) */"
    return spark.sql(f"""
        WITH u AS (
            SELECT user_id, year(yelping_since) AS reg_year, {ELITE_YEARS} AS elite_years
            FROM users
        ),
        stats AS (
            SELECT {hint_sql}
                   u.reg_year,
                   count(*)                       AS n_reviews,
                   avg(r.stars)                   AS avg_stars,
                   avg(r.stars - b.stars)         AS avg_strictness,
                   avg(CASE WHEN array_contains(u.elite_years, r.year) THEN 1 ELSE 0 END) * 100
                                                  AS pct_elite
            FROM review r
            JOIN u        ON r.user_id = u.user_id
            JOIN business b ON r.business_id = b.business_id
            GROUP BY u.reg_year
        ),
        no_reviews AS (
            SELECT u.reg_year, count(*) AS users_without_reviews
            FROM u LEFT ANTI JOIN review r ON u.user_id = r.user_id
            GROUP BY u.reg_year
        )
        SELECT coalesce(s.reg_year, n.reg_year)       AS reg_year,
               coalesce(s.n_reviews, 0)               AS n_reviews,
               round(s.avg_stars, 3)                  AS avg_stars,
               round(s.avg_strictness, 3)             AS avg_strictness,
               round(s.pct_elite, 2)                  AS pct_elite,
               coalesce(n.users_without_reviews, 0)   AS users_without_reviews
        FROM stats s FULL OUTER JOIN no_reviews n ON s.reg_year = n.reg_year
        ORDER BY reg_year
    """)


q = build_df() if api == "df" else build_sql()


def explain_str(df):
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        df.explain(mode="formatted")
    return buf.getvalue()


# SHUFFLE_REPLICATE_NL σε όλα τα δεδομένα: μόνο το πλάνο
if hint == "shuffle_replicate_nl":
    with open(os.path.join(RES, f"{tag}_explain.txt"), "w") as f:
        f.write(explain_str(q))
    print(f"[q3] {tag}: γράφτηκε μόνο το explain (χωρίς εκτέλεση)")
    spark.stop()
    sys.exit(0)

t0 = time.perf_counter()
rows = q.collect()
elapsed = time.perf_counter() - t0

with open(os.path.join(RES, "q3_times.csv"), "a") as f:
    f.write(f"{tag},{run},{elapsed:.3f}\n")
print(f"[q3] {tag} run={run} time={elapsed:.2f}s")

if run == "1":
    with open(os.path.join(RES, f"{tag}_result.txt"), "w") as f:
        f.write("\t".join(q.columns) + "\n")
        f.writelines("\t".join(str(v) for v in r) + "\n" for r in rows)
    with open(os.path.join(RES, f"{tag}_explain.txt"), "w") as f:
        f.write(explain_str(q))

    # Έλεγχος του πεδίου elite (Tip 3), εκτός χρονομέτρησης, μία φορά
    if api == "df" and hint == "none" and aqe == "aqe":
        tokens = (user_raw.select(F.explode(F.split(F.coalesce("elite", F.lit("")), ",")).alias("tok"))
                  .groupBy("tok").count().orderBy("tok"))
        anomalies = user_raw.filter(F.col("elite").rlike(r"(^|,)20,20(,|$)")).count()
        with open(os.path.join(RES, "q3_elite_check.txt"), "w") as f:
            f.write(f"Χρήστες με την ανωμαλία '20,20': {anomalies}\n\n")
            f.write("Διακριτά στοιχεία του elite (πριν τη διόρθωση) και πλήθος εμφανίσεων:\n")
            for r in tokens.collect():
                f.write(f"{r['tok']!r}\t{r['count']}\n")

spark.stop()
