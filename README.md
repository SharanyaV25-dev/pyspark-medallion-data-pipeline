# PySpark Medallion Data Pipeline

## Overview
Built an end-to-end batch data pipeline using PySpark implementing Medallion Architecture (Bronze, Silver, Gold) on smartphone usage and addiction data.

## Architecture
Raw CSV → Bronze → Silver → Gold

## Tech Stack
- PySpark
- Apache Spark
- Parquet
- Python

## Pipeline

### Bronze
- Raw CSV ingestion
- Added ingestion timestamp
- Added source file metadata

### Silver
- Null handling
- Deduplication
- Data standardization

### Gold
- User summary aggregation
- Addiction analysis
- Stress analysis

## Key Concepts
- Medallion Architecture
- Spark transformations/actions
- Parquet storage
- Partition-based processing

## Run
```bash
python src/main.py
```
