# Sales Data Sync Automation Pipeline
A serverless AWS pipeline that synchronizes sales data from CSV files uploaded to S3, aggregates totals using Lambda, and stores them in DynamoDB. Built with Python, AWS CLI, and GitHub Actions in the AWS Free Tier. Now supports real-world data from the Kaggle Online Retail Dataset.

## Objective
Align sales data across multiple systems to ensure accurate financial reporting, handling both mock and real-world datasets.

## Architecture
![Architecture Diagram](architecture.png)
- **S3**: Stores input CSV files in `s3://sales-data-sync/input/` and processed files in `s3://sales-data-sync/processed/year=YYYY/month=MM/`.
- **Lambda**: Processes uploaded CSVs, aggregates totals, and updates DynamoDB.
- **DynamoDB**: Stores aggregated sales totals (`id`, `total_amount`) with encryption enabled.
- **Step Functions**: Orchestrates the workflow (optional, for complex workflows).
- **CloudWatch**: Logs pipeline execution and monitors errors.
- **GitHub Actions**: Automates deployment of Lambda code.

## Design Decisions
- **Serverless**: Used Lambda and Step Functions for cost efficiency and scalability.
- **DynamoDB**: Chosen for low-latency updates and scalability over RDS; uses string IDs to support Kaggle data.
- **Partitioned S3**: Processed files are partitioned for efficient querying with Athena.
- **Kaggle Dataset**: Integrated Online Retail Dataset (10,000 records) to demonstrate scalability and real-world data handling.
- **Trade-offs**:
  - DynamoDB batch writes may face throttling with very large datasets; mitigated with retry logic.
  - CSV parsing assumes well-formed data; invalid rows are logged and skipped.

## Limitations
- Optimized for up to 10,000 records; larger datasets may require AWS Glue for ETL.
- Single-region deployment (af-south-1).

## Prerequisites
- AWS CLI configured with credentials.
- Python 3.9+.
- PowerShell (Windows) or Bash (Linux/Mac).
- IAM role with permissions (see `iam-policy.json`).

## Setup
1. **Create S3 Bucket**:
   ```bash
   aws s3api create-bucket --bucket sales-data-sync --region af-south-1 --create-bucket-configuration LocationConstraint=af-south-1
