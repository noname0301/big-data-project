
#!/usr/bin/env python3

import argparse
from pyspark.sql import SparkSession, functions as F

DATASETS = [
    "trips_by_hour",
    "daily_revenue",
    "zone_demand",
    "origin_destination",
    "payment_analysis",
]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--sample-rows", type=int, default=10)
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("Inspect-Taxi-Analytics")
        .getOrCreate()
    )

    try:
        for name in DATASETS:
            path = f"{args.input.rstrip('/')}/{name}"

            print("\n" + "=" * 70)
            print(f"DATASET: {name}")
            print(f"PATH: {path}")

            df = spark.read.parquet(path)

            print(f"Row count: {df.count():,}")
            print("\nSchema:")
            df.printSchema()

            print("Sample rows:")
            df.show(args.sample_rows, truncate=False)

            print("Null counts:")
            null_counts = df.select([
                F.count(
                    F.when(F.col(c).isNull(), 1)
                ).alias(c)
                for c in df.columns
            ])
            null_counts.show(truncate=False)

            if "trip_count" in df.columns:
                invalid_count = df.filter(
                    F.col("trip_count").isNull()
                    | (F.col("trip_count") <= 0)
                ).count()
                print(f"Invalid trip_count rows: {invalid_count}")

        print("\nInspection complete.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
