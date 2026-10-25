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

## Expected Outcome

The project aims to deliver a reproducible big data platform and benchmark report demonstrating how distributed processing, object storage, partitioning, and SQL serving affect performance as data volume and cluster size increase.

*Implementation status:* This diagram represents the intended target architecture. Components such as Airflow, Trino, the persistent Hive Metastore, and the dashboard are planned architecture components and should not be considered deployed until integration and validation are complete.