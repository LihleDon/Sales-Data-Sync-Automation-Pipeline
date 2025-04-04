# Sales Data Sync Automation Pipeline

A serverless AWS data pipeline that processes sales data from CSV files uploaded to S3, aggregates totals using Lambda, and stores them in DynamoDB. Built with Python and AWS CLI in the Free Tier.

## Architecture
- **S3**: Stores input CSV files in `s3://sales-data-sync/input/`.
- **Lambda**: Processes uploaded CSVs and updates totals incrementally.
- **DynamoDB**: Stores aggregated sales totals (`id`, `total_amount`).
- **S3 Trigger**: Automatically invokes Lambda on new `.csv` uploads.

## Prerequisites
- AWS CLI configured with credentials.
- Python 3.9+ installed locally.
- PowerShell (Windows).

## Setup and Usage
1. **Generate Sample Data**
   ```powershell
   python generate-sales-data.py