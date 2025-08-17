import pytest
import boto3
from moto import mock_s3, mock_dynamodb
from sync_sales_data import lambda_handler
import json
from datetime import datetime

@pytest.fixture
def s3_setup():
    with mock_s3():
        s3 = boto3.client('s3')
        s3.create_bucket(Bucket='test-bucket', CreateBucketConfiguration={'LocationConstraint': 'af-south-1'})
        # Mock Kaggle-like data
        csv_content = "id,amount\nINV001,100.5\nINV002,200.75\nINV003,-50.0\nINV004,invalid\n"
        s3.put_object(Bucket='test-bucket', Key='input/test.csv', Body=csv_content)
        yield s3

@pytest.fixture
def dynamodb_setup():
    with mock_dynamodb():
        dynamodb = boto3.resource('dynamodb', region_name='af-south-1')
        table = dynamodb.create_table(
            TableName='sales-totals',
            KeySchema=[{'AttributeName': 'id', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'id', 'AttributeType': 'S'}],  # String IDs
            BillingMode='PAY_PER_REQUEST'
        )
        table.meta.client.get_waiter('table_exists').wait(TableName='sales-totals')
        yield table

def test_lambda_handler(s3_setup, dynamodb_setup):
    event = {
        'Records': [{
            's3': {
                'bucket': {'name': 'test-bucket'},
                'object': {'key': 'input/test.csv'}
            }
        }]
    }
    response = lambda_handler(event, None)
    assert response['statusCode'] == 200
    assert "Processed 2 records" in response['body']  # Only valid records processed

    # Verify DynamoDB
    table = dynamodb_setup
    items = table.scan()['Items']
    assert len(items) == 2
    assert any(item['id'] == 'INV001' and float(item['total_amount']) == 100.5 for item in items)
    assert any(item['id'] == 'INV002' and float(item['total_amount']) == 200.75 for item in items)

    # Verify S3 partitioning
    year, month = datetime.now().strftime('%Y/%m').split('/')
    objects = s3_setup.list_objects_v2(Bucket='test-bucket', Prefix=f'processed/year={year}/month={month}/')
    assert len(objects.get('Contents', [])) == 1
