import boto3
import csv
from io import StringIO
from decimal import Decimal
import logging
from botocore.exceptions import ClientError
from retrying import retry

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb', region_name='af-south-1')

# Retry logic for transient errors
@retry(stop_max_attempt_number=3, wait_exponential_multiplier=1000, wait_exponential_max=10000)
def get_dynamodb_item(table, key):
    return table.get_item(Key=key)

def lambda_handler(event, context):
    try:
        # Get bucket and key from S3 event
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']
        logger.info(f"Processing file: s3://{bucket}/{key}")

        # Read the uploaded CSV from S3
        try:
            obj = s3_client.get_object(Bucket=bucket, Key=key)
            csv_content = obj['Body'].read().decode('utf-8')
        except ClientError as e:
            logger.error(f"Failed to read S3 file s3://{bucket}/{key}: {e}")
            raise

        # Parse CSV
        data = {}
        try:
            for row in csv.DictReader(StringIO(csv_content)):
                if not row.get('id') or not row.get('amount'):
                    logger.warning(f"Skipping invalid row: {row}")
                    continue
                data[int(row['id'])] = float(row['amount'])
        except csv.Error as e:
            logger.error(f"CSV parsing error for s3://{bucket}/{key}: {e}")
            raise

        # Write to DynamoDB
        table = dynamodb.Table('sales-totals')
        with table.batch_writer() as batch:
            for id, amount in data.items():
                try:
                    # Get existing total with retry
                    response = get_dynamodb_item(table, {'id': id})
                    current_total = Decimal('0')
                    if 'Item' in response:
                        current_total = Decimal(str(response['Item']['total_amount']))
                    new_total = current_total + Decimal(str(amount))
                    batch.put_item(Item={'id': id, 'total_amount': new_total})
                    logger.info(f"Updated ID {id} with new total {new_total}")
                except ClientError as e:
                    logger.error(f"Failed to update DynamoDB for ID {id}: {e}")
                    continue  # Skip failed items but continue processing

        return {
            'statusCode': 200,
            'body': f"Processed {len(data)} records from {key} into DynamoDB"
        }

    except Exception as e:
        logger.error(f"Unexpected error in Lambda: {e}", exc_info=True)
        raise
