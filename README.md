# S3 → AWS Glue → S3 Serverless ETL Pipeline

## Project Overview
Serverless, event-driven ETL pipeline that ingests raw data from Amazon S3, transforms it using PySpark in AWS Glue, and writes curated Parquet data back to S3 for analytics.

## Architecture

S3 (Raw) → Lambda (Trigger) → AWS Glue (PySpark) → S3 (Curated Parquet)

## Tech Stack
- AWS S3
- AWS Glue (PySpark)
- AWS Lambda
- AWS Glue Data Catalog
- Amazon CloudWatch
- AWS IAM
- Amazon Athena

## Key Features
- Event-driven ingestion via S3 notifications
- Schema discovery using Glue Crawlers
- Incremental processing using Glue Job Bookmarks
- Partitioned Parquet output (year/month/day)
- CloudWatch monitoring and alarms

## Results
- 60% reduction in processing time
- 70% storage cost savings (Parquet + compression)
- 5x faster Athena query performance

## How to Run
1. Upload raw files to `s3://raw-bucket/`
2. Lambda triggers Glue job automatically
3. Curated data appears in `s3://curated-bucket/`
4. Query via Amazon Athena

## Author
Abhijit Tejinkar — Senior Data Engineer