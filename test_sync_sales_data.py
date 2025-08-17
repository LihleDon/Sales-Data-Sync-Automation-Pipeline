import pytest
import boto3
from moto import mock_s3, mock_dynamodb
from sync_sales_data import lambda_handler
import json

@pytest.fixture
def s3_setup():
    with mock_s3():
        s3 = boto3.client('s3')
        s3.create_bucket(Bucket='test-bucket', CreateBucketConfiguration={'LocationConstraint': 'af-south-1'})
        csv_content = "id,amount\n1,100.5\n2,200.75\n"
        s3.put_object(Bucket='test-bucket', Key='input/test.csv', Body=csv_content)
        yield s3

@pytest.fixture
def dynamodb_setup():
    with mock_dynamodb():
        dynamodb = boto3.resource('dynamodb', region_name='af-south-1')
        table = dynamodb.create_table(
            TableName='sales-totals',
            KeySchema=[{'AttributeName': 'id', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'id', 'AttributeType': 'N'}],
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
    assert "Processed 2 records" in response['body']

    # Verify DynamoDB
    table = dynamodb_setup
    items = table.scan()['Items']
    assert len(items) == 2
    assert items[0]['id'] == 1
    assert items[0]['total_amount'] == 100.5

# Add more tests for edge cases (e.g., empty CSV, invalid data)
