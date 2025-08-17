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
                try:
                    # Handle string IDs and validate amount
                    id_val = str(row['id'])
                    amount = float(row['amount'])
                    if amount < 0:
                        logger.warning(f"Skipping negative amount for ID {id_val}")
                        continue
                    data[id_val] = amount
                except (ValueError, TypeError) as e:
                    logger.warning(f"Invalid data in row {row}: {e}")
                    continue
        except csv.Error as e:
            logger.error(f"CSV parsing error for s3://{bucket}/{key}: {e}")
            raise

        # Write to DynamoDB
        table = dynamodb.Table('sales-totals')
        with table.batch_writer() as batch:
            for id_val, amount in data.items():
                try:
                    # Get existing total with retry
                    response = get_dynamodb_item(table, {'id': id_val})
                    current_total = Decimal('0')
                    if 'Item' in response:
                        current_total = Decimal(str(response['Item']['total_amount']))
                    new_total = current_total + Decimal(str(amount))
                    batch.put_item(Item={'id': id_val, 'total_amount': new_total})
                    logger.info(f"Updated ID {id_val} with new total {new_total}")
                except ClientError as e:
                    logger.error(f"Failed to update DynamoDB for ID {id_val}: {e}")
                    continue

        # Move processed file to partitioned S3 location
        year, month = datetime.now().strftime('%Y/%m').split('/')
        processed_key = f"processed/year={year}/month={month}/{key.split('/')[-1]}"
        s3_client.copy_object(Bucket=bucket, Key=processed_key, CopySource={'Bucket': bucket, 'Key': key})
        s3_client.delete_object(Bucket=bucket, Key=key)
        logger.info(f"Moved file to s3://{bucket}/{processed_key}")

        return {
            'statusCode': 200,
            'body': f"Processed {len(data)} records from {key} into DynamoDB"
        }

    except Exception as e:
        logger.error(f"Unexpected error in Lambda: {e}", exc_info=True)
        raise
