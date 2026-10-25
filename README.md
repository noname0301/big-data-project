# Scalable Urban Mobility Analytics Platform

## Project Description

The Scalable Urban Mobility Analytics Platform is a big data engineering project designed to process, store, and analyze large-scale NYC TLC Yellow Taxi trip records. It aims to transform raw transportation data into reliable analytical datasets and provide efficient SQL access for exploring urban mobility patterns.

The platform uses a distributed architecture built around **Apache Spark** for data processing, **MinIO/S3-compatible object storage** for data persistence, **Parquet** for columnar storage, and **Trino** for interactive SQL analytics. A data catalog backed by Hive Metastore and PostgreSQL provides metadata management, while a dashboard layer enables users to visualize analytical results.

The data pipeline consists of four main stages:

1. **Data Ingestion:** Collect and store source datasets in an immutable raw data layer.
2. **Data Preparation:** Validate records, enforce data quality rules, derive useful features, and quarantine invalid records.
3. **Distributed Analytics:** Generate aggregated datasets for trip demand, revenue, taxi zone activity, origin-destination patterns, and payment behavior.
4. **SQL Serving and Visualization:** Expose analytical datasets through Trino and present insights through a dashboard.

## Project Objectives

- Build a modular and scalable big data processing pipeline.
- Maintain data quality, traceability, and reproducibility across processing stages.
- Evaluate distributed processing performance at different cluster sizes and data volumes.
- Compare partitioning strategies and their impact on query performance.
- Investigate fault tolerance and recovery during worker or executor failures.
- Deliver actionable insights into taxi demand, fares, and urban mobility patterns.

## Performance Evaluation

The project plans to benchmark workloads of approximately **5 GB, 20 GB, and 50 GB**, using configurations with **1, 2, and 4 Spark workers**. Experiments will measure processing time, query latency, resource utilization, and recovery behavior to understand how architecture and configuration choices affect performance.

The initial development workload uses January 2024 NYC TLC Yellow Taxi trip records. Additional data and infrastructure components will be integrated as the project progresses.

*Project status: This README describes the intended target architecture. Components and experiments should be marked as implemented only after they have been deployed and validated.*


# Dataset Description

## NYC TLC Yellow Taxi Trip Records

The platform uses the New York City Taxi and Limousine Commission (TLC) Yellow Taxi Trip Records dataset. Each record describes a taxi trip, including pickup and drop-off timestamps, trip distance, taxi zones, fare components, surcharges, and payment information.

**Source:** [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

**Format:** Parquet

**Initial development dataset:** January 2024 Yellow Taxi trip records.

## Schema and Field Definitions

| Field | Description |
|---|---|
| `VendorID` | TPEP provider identifier: `1` = Creative Mobile Technologies, `2` = Curb Mobility, `6` = Myle Technologies, `7` = Helix. |
| `tpep_pickup_datetime` | Date and time when the meter was engaged. |
| `tpep_dropoff_datetime` | Date and time when the meter was disengaged. |
| `passenger_count` | Number of passengers in the vehicle. |
| `trip_distance` | Elapsed trip distance in miles reported by the taximeter. |
| `RatecodeID` | Final rate code: `1` = Standard, `2` = JFK, `3` = Newark, `4` = Nassau/Westchester, `5` = Negotiated fare, `6` = Group ride, `99` = Null/unknown. |
| `store_and_fwd_flag` | Whether the record was stored in vehicle memory before transmission: `Y` = Yes, `N` = No. |
| `PULocationID` | TLC Taxi Zone where the meter was engaged (pickup). |
| `DOLocationID` | TLC Taxi Zone where the meter was disengaged (drop-off). |
| `payment_type` | Payment code: `0` = Flex Fare, `1` = Credit card, `2` = Cash, `3` = No charge, `4` = Dispute, `5` = Unknown, `6` = Voided trip. |
| `fare_amount` | Time-and-distance fare calculated by the meter. |
| `extra` | Miscellaneous extras and surcharges. |
| `mta_tax` | Tax automatically triggered based on the metered rate. |
| `tip_amount` | Recorded tip amount. Automatically populated for credit card tips; cash tips are excluded. |
| `tolls_amount` | Total tolls paid during the trip. |
| `improvement_surcharge` | Improvement surcharge assessed at the flag drop. |
| `total_amount` | Total amount charged to passengers, excluding cash tips. |
| `congestion_surcharge` | Total New York State congestion surcharge collected for the trip. |
| `airport_fee` | Airport fee associated with pickups at LaGuardia and John F. Kennedy airports. |

## Data Processing and Quality

The dataset is processed through three layers:

1. **Raw layer:** Preserves source records in object storage.
2. **Curated layer:** Applies schema validation, type casting, timestamp and distance checks, and other quality rules. Invalid records are written to a quarantine dataset.
3. **Analytics layer:** Generates aggregated datasets for hourly trip demand, daily revenue, taxi zone demand, origin-destination flows, and payment analysis.

## Analytical Use Cases

- Analyze trip demand by hour, date, and day of week.
- Measure fare revenue, surcharges, and recorded tips.
- Compare pickup and drop-off activity across TLC Taxi Zones.
- Identify frequently traveled origin-destination pairs.
- Compare trip characteristics across payment types and rate codes.

## Dataset Considerations

- Pickup and drop-off timestamps should be interpreted in New York local time, with daylight-saving transitions considered.
- `tip_amount` and `total_amount` do not include cash tips.
- `PULocationID` and `DOLocationID` represent taxi zones, not precise geographic coordinates.
- Provider, payment, and rate codes should be interpreted according to their documented meanings; unknown or exceptional codes should not automatically be treated as valid categories.
- The January 2024 dataset is the initial development workload. Larger datasets and scaled workloads will be used for performance experiments at approximately 5 GB, 20 GB, and 50 GB.

For fare and surcharge details, refer to the [NYC TLC taxi fare information](https://www.nyc.gov/site/tlc/passengers/taxi-fare.page).

# Overall System Architecture

## Overview

The Scalable Urban Mobility Analytics Platform is designed to process large-scale NYC TLC taxi trip data using a distributed big data architecture. The project focuses on scalability, data quality, analytical query performance, fault tolerance, and urban mobility insights.

## Architecture

```text
          NYC TLC Trip Data
                  |
                  v
       Ingestion & Orchestration
          (Python / Airflow)
                  |
                  v
       Immutable Raw Data Layer
             (MinIO / S3)
                  |
                  v
      Data Validation & Cleaning
             (Apache Spark)
             /           \
            v             v
    Curated Data       Quarantine
       (Parquet)      Invalid Records
            |
            v
    Distributed Analytics
       (Apache Spark)
            |
            v
     Analytics Data Layer
   (Partitioned Parquet / S3)
            |
            v
      SQL / OLAP Serving
          (Trino)
            |
            v
      Visualization Layer
    (Apache Superset / Grafana)

Supporting Services:
PostgreSQL + Hive Metastore | Docker Compose
```

## Main Components

- **Ingestion and orchestration:** Python scripts and, in the planned full architecture, Apache Airflow manage monthly data ingestion, retries, and ingestion tracking.
- **Object storage:** MinIO provides S3-compatible storage for immutable raw data, curated datasets, and analytical outputs.
- **Data processing:** Apache Spark performs distributed cleaning, validation, feature engineering, and analytical aggregation.
- **Data catalog:** Hive Metastore with PostgreSQL stores table metadata so SQL engines can discover datasets in object storage.
- **SQL serving:** Trino provides interactive SQL access to analytical datasets without requiring dashboard clients to run Spark jobs directly.
- **Visualization:** Apache Superset or Grafana presents mobility trends, demand hotspots, origin-destination flows, and other analytical results.
- **Infrastructure:** Docker Compose manages services and supports reproducible deployment and worker configuration.

## Scalability and Reliability Evaluation

The platform is intended to evaluate:

- **Worker scalability:** Compare 1, 2, and 4 Spark workers.
- **Data scalability:** Benchmark workloads at approximately 5 GB, 20 GB, and 50 GB.
- **Partitioning strategies:** Compare monthly, daily, and date-plus-zone partitioning using query latency, data scanned, file counts, and execution plans.
- **Fault tolerance:** Terminate a worker or executor during processing and measure recovery, recomputation, and output consistency.