#!/usr/bin/env python3
"""Create five analytics aggregates from curated NYC TLC Yellow Taxi Parquet data."""
import argparse
import sys
from pyspark.sql import SparkSession, functions as F


def parse_args():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--input", required=True, help="Curated Parquet input path")
    p.add_argument("--output", required=True, help="Analytics output root")
    p.add_argument("--timezone", default="America/New_York")
    p.add_argument("--overwrite", action="store_true",
                   help="Replace existing output datasets (use carefully)")
    return p.parse_args()


def require_columns(df, names):
    missing = [c for c in names if c not in df.columns]
    if missing:
        raise ValueError(
            "Missing required columns: " + ", ".join(missing)
            + "\nAvailable columns: " + ", ".join(df.columns)
        )


def main():
    args = parse_args()
    mode = "overwrite" if args.overwrite else "errorifexists"
    spark = SparkSession.builder.appName("NYC-Taxi-Five-Aggregates").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", args.timezone)
    trips = None

    try:
        print(f"Reading curated data: {args.input}")
        source = spark.read.parquet(args.input)
        if "is_valid" in source.columns:
            source = source.filter(F.col("is_valid") == F.lit(True))

        require_columns(source, [
            "tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance",
            "PULocationID", "DOLocationID", "fare_amount", "total_amount",
            "tip_amount", "payment_type",
        ])

        trips = (
            source
            .withColumn("pickup_ts", F.to_timestamp("tpep_pickup_datetime"))
            .withColumn("dropoff_ts", F.to_timestamp("tpep_dropoff_datetime"))
            .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
            .withColumn("pickup_year", F.year("tpep_pickup_datetime"))
            .withColumn("pickup_month", F.month("tpep_pickup_datetime"))
            .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
            .withColumn("pickup_day_of_week", F.dayofweek("tpep_pickup_datetime"))
            .withColumn(
                "trip_duration_minutes",
                (F.col("dropoff_ts").cast("long") - F.col("pickup_ts").cast("long")) / 60.0,
            )
            .withColumn("trip_distance_num", F.col("trip_distance").cast("double"))
            .withColumn("fare_amount_num", F.col("fare_amount").cast("double"))
            .withColumn("total_amount_num", F.col("total_amount").cast("double"))
            .withColumn("tip_amount_num", F.col("tip_amount").cast("double"))
            .withColumn("payment_type_num", F.col("payment_type").cast("int"))
            .withColumn("pu_zone", F.col("PULocationID").cast("int"))
            .withColumn("do_zone", F.col("DOLocationID").cast("int"))
            .filter(
                F.col("pickup_ts").isNotNull()
                & F.col("dropoff_ts").isNotNull()
                & (F.col("dropoff_ts") >= F.col("pickup_ts"))
            )
            .cache()
        )

        input_count = source.count()
        usable_count = trips.count()
        print(f"Curated input rows: {input_count:,}")
        print(f"Rows with usable timestamps: {usable_count:,}")

        # 1. Demand by date and hour.
        trips_by_hour = (
            trips.groupBy("pickup_date", "pickup_year", "pickup_month",
                          "pickup_day_of_week", "pickup_hour")
            .agg(
                F.count("*").alias("trip_count"),
                F.avg("trip_duration_minutes").alias("avg_duration_minutes"),
                F.expr("percentile_approx(trip_duration_minutes, 0.5, 10000)")
                 .alias("median_duration_minutes"),
                F.avg("trip_distance_num").alias("avg_trip_distance_miles"),
            )
        )

        # 2. Daily financial metrics. total_amount is the recorded total charge.
        daily_revenue = (
            trips.groupBy("pickup_date", "pickup_year", "pickup_month")
            .agg(
                F.count("*").alias("trip_count"),
                F.sum("fare_amount_num").alias("sum_fare_amount"),
                F.sum("total_amount_num").alias("sum_total_amount"),
                F.sum("tip_amount_num").alias("sum_tip_amount"),
                F.avg("fare_amount_num").alias("avg_fare_amount"),
                F.avg("total_amount_num").alias("avg_total_amount"),
                F.avg("tip_amount_num").alias("avg_tip_amount"),
            )
            .withColumn(
                "tip_to_fare_ratio",
                F.when(F.col("sum_fare_amount") > 0,
                       F.col("sum_tip_amount") / F.col("sum_fare_amount")),
            )
        )

        # 3. Pickup and drop-off zone demand in a normalized table.
        pickup_demand = (
            trips.groupBy("pickup_year", "pickup_month",
                          F.col("pu_zone").alias("zone_id"))
            .agg(F.count("*").alias("trip_count"))
            .withColumn("zone_role", F.lit("pickup"))
        )
        dropoff_demand = (
            trips.groupBy("pickup_year", "pickup_month",
                          F.col("do_zone").alias("zone_id"))
            .agg(F.count("*").alias("trip_count"))
            .withColumn("zone_role", F.lit("dropoff"))
        )
        zone_demand = pickup_demand.unionByName(dropoff_demand).select(
            "pickup_year", "pickup_month", "zone_role", "zone_id", "trip_count"
        )

        # 4. Origin-destination pairs.
        origin_destination = (
            trips.groupBy("pickup_year", "pickup_month", "pu_zone", "do_zone")
            .agg(
                F.count("*").alias("trip_count"),
                F.avg("trip_distance_num").alias("avg_trip_distance_miles"),
                F.avg("trip_duration_minutes").alias("avg_duration_minutes"),
                F.avg("total_amount_num").alias("avg_total_amount"),
            )
            .withColumnRenamed("pu_zone", "pickup_zone_id")
            .withColumnRenamed("do_zone", "dropoff_zone_id")
        )

        # 5. Trip profile by TLC payment-type code.
        payment_analysis = (
            trips.groupBy("pickup_year", "pickup_month", "payment_type_num")
            .agg(
                F.count("*").alias("trip_count"),
                F.avg("trip_distance_num").alias("avg_trip_distance_miles"),
                F.avg("trip_duration_minutes").alias("avg_duration_minutes"),
                F.avg("fare_amount_num").alias("avg_fare_amount"),
                F.avg("total_amount_num").alias("avg_total_amount"),
                F.avg("tip_amount_num").alias("avg_tip_amount"),
                F.sum("tip_amount_num").alias("sum_tip_amount"),
            )
            .withColumnRenamed("payment_type_num", "payment_type")
        )

        outputs = [
            ("trips_by_hour", trips_by_hour),
            ("daily_revenue", daily_revenue),
            ("zone_demand", zone_demand),
            ("origin_destination", origin_destination),
            ("payment_analysis", payment_analysis),
        ]

        print("\nWriting aggregate datasets...")
        for name, df in outputs:
            path = f"{args.output.rstrip('/')}/{name}"
            count = df.count()
            print(f"\n{name}: {count:,} rows -> {path}")
            df.write.mode(mode).parquet(path)

        print("\nVerifying each output by reading it back...")
        for name, df in outputs:
            path = f"{args.output.rstrip('/')}/{name}"
            actual = spark.read.parquet(path).count()
            expected = df.count()
            if actual != expected:
                raise RuntimeError(
                    f"{name}: row count mismatch; expected {expected}, got {actual}"
                )
            print(f"PASS {name}: {actual:,} rows")

        print("\nSUCCESS: all five aggregate datasets were written and verified.")
        print(f"Analytics root: {args.output}")

    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise
    finally:
        if trips is not None:
            trips.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()
