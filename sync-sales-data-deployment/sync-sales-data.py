import boto3
import csv
from io import StringIO
from decimal import Decimal

s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb', region_name='af-south-1')

def lambda_handler(event, context):
    # Get bucket and key from S3 event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']

    # Read the uploaded CSV from S3
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    data = {}
    for row in csv.DictReader(StringIO(obj['Body'].read().decode('utf-8'))):
        data[int(row['id'])] = float(row['amount'])

    # Write to DynamoDB (update existing totals if present)
    table = dynamodb.Table('sales-totals')
    with table.batch_writer() as batch:
        for id, amount in data.items():
            # Get existing total (if any) and add new amount
            response = table.get_item(Key={'id': id})
            current_total = Decimal('0')
            if 'Item' in response:
                current_total = Decimal(str(response['Item']['total_amount']))
            new_total = current_total + Decimal(str(amount))
            batch.put_item(Item={'id': id, 'total_amount': new_total})

    return {
        'statusCode': 200,
        'body': f"Processed {len(data)} records from {key} into DynamoDB"
    }