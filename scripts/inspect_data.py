from pathlib import Path

import pyarrow.parquet as pq

DATA_FILE = Path(
    "data/yellow_tripdata_2024-01.parquet"
)


def main() -> None:
    if not DATA_FILE.exists():
        raise FileNotFoundError(
            f"Dataset not found: {DATA_FILE}"
        )

    parquet_file = pq.ParquetFile(DATA_FILE)

    print(f"File: {DATA_FILE}")
    print(f"Rows: {parquet_file.metadata.num_rows:,}")
    print(f"Row groups: {parquet_file.num_row_groups}")
    print("\nSchema:")
    print(parquet_file.schema)

    table = pq.read_table(DATA_FILE, columns=[
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
    ])

    print(table.schema)
    print(table.slice(0, 5).to_pandas())


if __name__ == "__main__":
    main()