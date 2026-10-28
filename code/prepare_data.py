import os
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (StructType, StructField, StringType,
                               DoubleType, IntegerType, LongType)

BASE = os.path.expanduser("~/advdb")
RAW = "file://" + os.path.join(BASE, "raw")
OUT = "file://" + os.path.join(BASE, "parquet")

spark = SparkSession.builder.appName("prepare_data").getOrCreate()

# business: attributes/hours ως STRING -> raw JSON για parse_json αργότερα
business_schema = StructType([
    StructField("business_id", StringType()), StructField("name", StringType()),
    StructField("address", StringType()), StructField("city", StringType()),
    StructField("state", StringType()), StructField("postal_code", StringType()),
    StructField("latitude", DoubleType()), StructField("longitude", DoubleType()),
    StructField("stars", DoubleType()), StructField("review_count", IntegerType()),
    StructField("is_open", IntegerType()), StructField("attributes", StringType()),
    StructField("categories", StringType()), StructField("hours", StringType()),
])
(spark.read.schema(business_schema).json(f"{RAW}/yelp_academic_dataset_business.json")
 .write.mode("overwrite").parquet(f"{OUT}/business"))

# review: partitionBy έτος
review_schema = StructType([
    StructField("review_id", StringType()), StructField("user_id", StringType()),
    StructField("business_id", StringType()), StructField("stars", DoubleType()),
    StructField("useful", IntegerType()), StructField("funny", IntegerType()),
    StructField("cool", IntegerType()), StructField("text", StringType()),
    StructField("date", StringType()),
])
(spark.read.schema(review_schema).json(f"{RAW}/yelp_academic_dataset_review.json")
 .withColumn("date", F.try_to_timestamp(F.col("date"), F.lit("yyyy-MM-dd HH:mm:ss")))
 .withColumn("year", F.year("date"))
 .write.mode("overwrite").partitionBy("year").parquet(f"{OUT}/review"))

# user
user_schema = StructType([
    StructField("user_id", StringType()), StructField("name", StringType()),
    StructField("review_count", IntegerType()), StructField("yelping_since", StringType()),
    StructField("useful", IntegerType()), StructField("funny", IntegerType()),
    StructField("cool", IntegerType()), StructField("elite", StringType()),
    StructField("friends", StringType()), StructField("fans", IntegerType()),
    StructField("average_stars", DoubleType()),
])
(spark.read.schema(user_schema).json(f"{RAW}/yelp_academic_dataset_user.json")
 .withColumn("yelping_since",
             F.try_to_timestamp(F.col("yelping_since"), F.lit("yyyy-MM-dd HH:mm:ss")))
 .write.mode("overwrite").parquet(f"{OUT}/user"))

# GloVe: text -> word STRING, vec ARRAY<FLOAT>
parts = F.split(F.col("value"), " ")
(spark.read.text(f"{RAW}/glove.6B.100d.txt")
 .select(parts.getItem(0).alias("word"),
         F.transform(F.slice(parts, 2, 100), lambda x: x.cast("float")).alias("vec"))
 .write.mode("overwrite").parquet(f"{OUT}/glove"))

# ACS: zcta ως STRING (κρατά το αρχικό μηδέν)
acs_schema = StructType([
    StructField("zcta", StringType()),
    StructField("median_household_income", LongType()),
    StructField("population", LongType()),
])
(spark.read.csv(f"{RAW}/acs_zcta.csv", sep=";", header=True, schema=acs_schema)
 .write.mode("overwrite").parquet(f"{OUT}/acs"))

# έλεγχοι
for name in ["business", "review", "user", "glove", "acs"]:
    df = spark.read.parquet(f"{OUT}/{name}")
    print(f"=== {name}: {df.count()} γραμμές")
    df.printSchema()
g = spark.read.parquet(f"{OUT}/glove")
print("GloVe με vec μήκους != 100:", g.filter(F.size("vec") != 100).count())
print("Reviews χωρίς έτος:",
      spark.read.parquet(f"{OUT}/review").filter(F.col("year").isNull()).count())

spark.stop()
