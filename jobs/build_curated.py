#!/usr/bin/env python3
"""
Curate NYC TLC Yellow Taxi monthly Parquet data from S3A.

Example:
spark-submit --master spark://spark-master:7077 \
  --properties-file /opt/spark/conf/spark-defaults.conf \
  /opt/spark-apps/build_curated.py \
  --input s3a://taxi/raw/yellow_tripdata/year=2024/month=01/yellow_tripdata_2024-01.parquet \
  --output s3a://taxi/curated-test/yellow_tripdata/year=2024/month=01/run=v1 \
  --quarantine s3a://taxi/quarantine-test/yellow_tripdata/year=2024/month=01/run=v1 \
  --version v1 --timezone America/New_York
"""

import argparse
from functools import reduce

from pyspark.sql import SparkSession, functions as F
from pyspark.storagelevel import StorageLevel


SOURCE_COLUMNS = [
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "RatecodeID",
    "store_and_fwd_flag",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "airport_fee",
]

DOUBLE_COLUMNS = [
    "trip_distance",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "airport_fee",
]

OPTIONAL_COLUMNS = {
    "passenger_count",
    "RatecodeID",
    "store_and_fwd_flag",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "congestion_surcharge",
    "airport_fee",
}


def parse_args():
    p = argparse.ArgumentParser(description="Curate TLC Yellow Taxi Parquet data")
    p.add_argument("--input", required=True, help="Input raw Parquet path (S3A)")
    p.add_argument("--output", required=True, help="Output curated Parquet path (S3A)")
    p.add_argument("--quarantine", required=True, help="Output invalid-record path (S3A)")
    p.add_argument("--version", default="v1")
    p.add_argument("--timezone", default="America/New_York")
    p.add_argument(
        "--max-duration-hours",
        type=float,
        default=24.0,
        help="Duration above this is flagged as unusual, not rejected",
    )
    p.add_argument(
        "--max-distance-miles",
        type=float,
        default=200.0,
        help="Distance above this is flagged as unusual, not rejected",
    )
    return p.parse_args()


def main():
    args = parse_args()
    spark = (
        SparkSession.builder
        .appName("NYC-TLC-Yellow-Taxi-Curated")
        .getOrCreate()
    )
    spark.conf.set("spark.sql.session.timeZone", args.timezone)

    # Read the source schema and normalize column names case-insensitively.
    raw = spark.read.parquet(args.input)
    existing = {name.lower(): name for name in raw.columns}
    for expected in SOURCE_COLUMNS:
        actual = existing.get(expected.lower())
        if actual is None:
            if expected not in OPTIONAL_COLUMNS:
                raise ValueError(
                    f"Required source column {expected!r} missing. "
                    f"Available columns: {raw.columns}"
                )
            raw = raw.withColumn(expected, F.lit(None).cast("string"))
        elif actual != expected:
            raw = raw.withColumnRenamed(actual, expected)

    # Keep the source fields, but cast them to stable analytical types.
    df = raw.select(*SOURCE_COLUMNS).withColumn(
        "source_file", F.input_file_name()
    )

    df = (
        df
        .withColumn("VendorID", F.col("VendorID").cast("int"))
        .withColumn(
            "pickup_ts",
            F.col("tpep_pickup_datetime").cast("timestamp")
        )
        .withColumn(
            "dropoff_ts",
            F.col("tpep_dropoff_datetime").cast("timestamp")
        )
        .withColumn("passenger_count", F.col("passenger_count").cast("int"))
        .withColumn("RatecodeID", F.col("RatecodeID").cast("int"))
        .withColumn("PULocationID", F.col("PULocationID").cast("int"))
        .withColumn("DOLocationID", F.col("DOLocationID").cast("int"))
        .withColumn("payment_type", F.col("payment_type").cast("int"))
        .withColumn(
            "store_and_fwd_flag",
            F.upper(F.trim(F.col("store_and_fwd_flag").cast("string")))
        )
    )

    for name in DOUBLE_COLUMNS:
        df = df.withColumn(name, F.col(name).cast("double"))

    # Derived temporal and trip metrics. fare_per_mile is undefined
    # when distance is null or non-positive.
    duration = (
        F.col("dropoff_ts").cast("long")
        - F.col("pickup_ts").cast("long")
    )
    df = (
        df
        .withColumn("duration_seconds", duration.cast("long"))
        .withColumn("duration_minutes", F.col("duration_seconds") / F.lit(60.0))
        .withColumn(
            "fare_per_mile",
            F.when(
                F.col("trip_distance") > 0,
                F.col("fare_amount") / F.col("trip_distance")
            )
        )
        .withColumn("pickup_date", F.to_date("pickup_ts"))
        .withColumn("pickup_year", F.year("pickup_ts"))
        .withColumn("pickup_month", F.month("pickup_ts"))
        .withColumn("pickup_hour", F.hour("pickup_ts"))
        # Spark dayofweek: Sunday=1, Saturday=7.
        .withColumn("day_of_week", F.dayofweek("pickup_ts"))
        .withColumn("is_weekend", F.dayofweek("pickup_ts").isin([1, 7]))
    )

    # Hard-invalid rules. Zone validation here checks for present positive
    # IDs; use the official TLC zone lookup for authoritative membership checks.
    invalid = {
        "missing_pickup_timestamp": F.col("pickup_ts").isNull(),
        "missing_dropoff_timestamp": F.col("dropoff_ts").isNull(),
        "dropoff_before_pickup": (
            F.col("dropoff_ts").isNotNull()
            & F.col("pickup_ts").isNotNull()
            & (F.col("dropoff_ts") < F.col("pickup_ts"))
        ),
        "missing_or_invalid_pickup_zone": (
            F.col("PULocationID").isNull() | (F.col("PULocationID") <= 0)
        ),
        "missing_or_invalid_dropoff_zone": (
            F.col("DOLocationID").isNull() | (F.col("DOLocationID") <= 0)
        ),
        "missing_or_invalid_trip_distance": (
            F.col("trip_distance").isNull()
            | F.isnan("trip_distance")
            | (F.abs(F.col("trip_distance")) == F.lit(float("inf")))
            | (F.col("trip_distance") < 0)
        ),
    }

    # Negative fares and totals are flagged, not rejected, because they may
    # represent adjustments. Missing optional fare components are allowed.
    df = (
        df
        .withColumn(
            "is_negative_fare",
            F.coalesce(F.col("fare_amount") < 0, F.lit(False))
        )
        .withColumn(
            "is_negative_total",
            F.coalesce(F.col("total_amount") < 0, F.lit(False))
        )
        .withColumn(
            "is_zero_distance",
            F.coalesce(F.col("trip_distance") == 0, F.lit(False))
        )
        .withColumn(
            "is_extreme_duration",
            F.coalesce(
                F.col("duration_minutes") > args.max_duration_hours * 60,
                F.lit(False)
            )
        )
        .withColumn(
            "is_extreme_distance",
            F.coalesce(
                F.col("trip_distance") > args.max_distance_miles,
                F.lit(False)
            )
        )
        .withColumn(
            "is_unknown_vendor",
            F.col("VendorID").isNotNull()
            & ~F.col("VendorID").isin([1, 2, 6, 7])
        )
        .withColumn(
            "is_unknown_payment_type",
            F.col("payment_type").isNotNull()
            & ~F.col("payment_type").isin([0, 1, 2, 3, 4, 5, 6])
        )
        .withColumn(
            "is_unknown_ratecode",
            F.col("RatecodeID").isNotNull()
            & ~F.col("RatecodeID").isin([1, 2, 3, 4, 5, 6, 99])
        )
    )

    reason_exprs = [
        F.when(condition, F.lit(reason))
        for reason, condition in invalid.items()
    ]
    is_valid_expr = reduce(
        lambda left, right: left & ~F.coalesce(right, F.lit(False)),
        invalid.values(),
        F.lit(True),
    )

    warning_expr = (
        F.col("is_negative_fare")
        | F.col("is_negative_total")
        | F.col("is_zero_distance")
        | F.col("is_extreme_duration")
        | F.col("is_extreme_distance")
        | F.col("is_unknown_vendor")
        | F.col("is_unknown_payment_type")
        | F.col("is_unknown_ratecode")
    )

    df = (
        df
        .withColumn("quality_reasons", F.concat_ws("|", *reason_exprs))
        .withColumn("is_valid", is_valid_expr)
        .withColumn(
            "quality_status",
            F.when(~F.col("is_valid"), F.lit("invalid"))
             .when(warning_expr, F.lit("valid_with_warnings"))
             .otherwise(F.lit("valid"))
        )
        .withColumn("processing_version", F.lit(args.version))
        .withColumn("ingested_at", F.current_timestamp())
    )

    # Cache so counts and both output writes use the same evaluated records.
    df = df.persist(StorageLevel.MEMORY_AND_DISK)
    input_count = df.count()
    valid = df.filter(F.col("is_valid"))
    quarantine = df.filter(~F.col("is_valid"))
    curated_count = valid.count()
    quarantine_count = quarantine.count()

    if input_count != curated_count + quarantine_count:
        raise RuntimeError(
            "Reconciliation failed: input rows != curated rows + quarantine rows"
        )

    # errorifexists prevents accidental overwrites. Use a new run path or
    # explicitly clean a test output path before rerunning.
    (
        valid.write
        .mode("errorifexists")
        .partitionBy("pickup_year", "pickup_month")
        .parquet(args.output)
    )
    (
        quarantine.write
        .mode("errorifexists")
        .parquet(args.quarantine)
    )

    # Read-back smoke test: ensure curated output is readable and non-empty
    # whenever valid records exist.
    check = spark.read.parquet(args.output)
    readback_count = check.count()
    if readback_count != curated_count:
        raise RuntimeError(
            f"Read-back count mismatch: wrote {curated_count}, read {readback_count}"
        )

    print("=== CURATION TEST SUMMARY ===")
    print(f"Input rows:      {input_count}")
    print(f"Curated rows:    {curated_count}")
    print(f"Quarantined rows:{quarantine_count}")
    print(f"Read-back rows:  {readback_count}")
    print(f"Curated output:  {args.output}")
    print(f"Quarantine path: {args.quarantine}")
    print("Reconciliation:  PASS")
    print("Parquet readback: PASS")

    print("Quality status counts:")
    df.groupBy("quality_status").count().orderBy("quality_status").show()

    df.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
