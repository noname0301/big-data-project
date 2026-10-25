
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("Inspect-Taxi-Quarantine")
    .getOrCreate()
)

spark.conf.set("spark.sql.session.timeZone", "America/New_York")

path = (
    "s3a://taxi/quarantine-test/yellow_tripdata/"
    "year=2024/month=01/run=v1"
)

df = spark.read.parquet(path)

print("=== QUARANTINE SCHEMA ===")
df.printSchema()

print("=== TOTAL QUARANTINED ROWS ===")
print(df.count())

print("=== QUARANTINED ROWS ===")
df.select(
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "trip_distance",
    "PULocationID",
    "DOLocationID",
    "quality_reasons",
    "source_file"
).show(50, truncate=False)

print("=== COUNTS BY QUARANTINE REASON ===")
df.groupBy("quality_reasons").count().orderBy(
    F.desc("count")
).show(100, truncate=False)

spark.stop()
