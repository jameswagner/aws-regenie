import pytest
import json
import boto3
from moto import mock_dynamodb
from unittest.mock import patch
from datetime import datetime

# Import the function under test
from src.lambdas.success_handler.lambda_function import handler


@pytest.fixture
def setup_environment():
    """Set up environment variables for testing"""
    with patch.dict('os.environ', {
        'WORKFLOW_TABLE_NAME': 'test-workflow-table',
        'JOB_STATUS_TABLE_NAME': 'test-job-status-table'
    }):
        yield


@pytest.fixture
def workflow_table_name():
    return 'test-workflow-table'


@pytest.fixture
def job_status_table_name():
    return 'test-job-status-table'


@mock_dynamodb
def test_success_handler_basic(setup_environment, workflow_table_name, job_status_table_name):
    # Set up mock DynamoDB tables
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    
    # Create workflow table
    workflow_table = dynamodb.create_table(
        TableName=workflow_table_name,
        KeySchema=[
            {'AttributeName': 'workflowId', 'KeyType': 'HASH'}
        ],
        AttributeDefinitions=[
            {'AttributeName': 'workflowId', 'AttributeType': 'S'}
        ],
        BillingMode='PAY_PER_REQUEST'
    )
    
    # Create job status table
    job_status_table = dynamodb.create_table(
        TableName=job_status_table_name,
        KeySchema=[
            {'AttributeName': 'workflowId', 'KeyType': 'HASH'},
            {'AttributeName': 'jobId', 'KeyType': 'RANGE'}
        ],
        AttributeDefinitions=[
            {'AttributeName': 'workflowId', 'AttributeType': 'S'},
            {'AttributeName': 'jobId', 'AttributeType': 'S'}
        ],
        BillingMode='PAY_PER_REQUEST'
    )
    
    # Create test workflow record
    workflow_id = "test-workflow-123"
    timestamp = datetime.utcnow().isoformat()
    workflow_table.put_item(
        Item={
            'workflowId': workflow_id,
            'createdAt': timestamp,
            'updatedAt': timestamp,
            'status': 'IN_PROGRESS'
        }
    )
    
    # Create test job records (all completed)
    jobs = [
        {
            'workflowId': workflow_id,
            'jobId': f'{workflow_id}-step1',
            'status': 'COMPLETED',
            'stepNumber': 1,
            'updatedAt': timestamp
        },
        {
            'workflowId': workflow_id,
            'jobId': f'{workflow_id}-step2-chr1',
            'status': 'COMPLETED',
            'stepNumber': 2,
            'updatedAt': timestamp
        },
        {
            'workflowId': workflow_id,
            'jobId': f'{workflow_id}-step2-chr2',
            'status': 'COMPLETED',
            'stepNumber': 2,
            'updatedAt': timestamp
        }
    ]
    
    for job in jobs:
        job_status_table.put_item(Item=job)
    
    # Create test event
    event = {
        'workflowId': workflow_id,
        'resultsBucketPath': 's3://test-bucket/results/',
        'completionTime': timestamp
    }
    
    # Call the handler
    result = handler(event, {})
    
    # Verify the result
    assert result['workflowId'] == workflow_id
    assert result['status'] == 'COMPLETED'
    assert result['jobStats']['total'] == 3
    assert result['jobStats']['completed'] == 3
    assert result['jobStats']['failed'] == 0
    assert result['jobStats']['pending'] == 0
    assert result['resultsBucketPath'] == 's3://test-bucket/results/'
    assert result['message'] == 'Workflow completed successfully'
    
    # Verify workflow status was updated in DynamoDB
    workflow = workflow_table.get_item(
        Key={'workflowId': workflow_id}
    )['Item']
    assert workflow['status'] == 'COMPLETED'
    assert 'jobStats' in workflow
    assert workflow['jobStats']['total'] == 3
    assert workflow['jobStats']['completed'] == 3
    assert workflow['resultsBucketPath'] == 's3://test-bucket/results/'
    assert workflow['completionTime'] == timestamp


@mock_dynamodb
def test_success_handler_missing_workflow_id(setup_environment, workflow_table_name, job_status_table_name):
    # Set up mock DynamoDB tables
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    
    # Create tables
    workflow_table = dynamodb.create_table(
        TableName=workflow_table_name,
        KeySchema=[
            {'AttributeName': 'workflowId', 'KeyType': 'HASH'}
        ],
        AttributeDefinitions=[
            {'AttributeName': 'workflowId', 'AttributeType': 'S'}
        ],
        BillingMode='PAY_PER_REQUEST'
    )
    
    job_status_table = dynamodb.create_table(
        TableName=job_status_table_name,
        KeySchema=[
            {'AttributeName': 'workflowId', 'KeyType': 'HASH'},
            {'AttributeName': 'jobId', 'KeyType': 'RANGE'}
        ],
        AttributeDefinitions=[
            {'AttributeName': 'workflowId', 'AttributeType': 'S'},
            {'AttributeName': 'jobId', 'AttributeType': 'S'}
        ],
        BillingMode='PAY_PER_REQUEST'
    )
    
    # Create test event with missing workflow ID
    event = {
        'resultsBucketPath': 's3://test-bucket/results/'
    }
    
    # Call the handler and expect it to raise an exception
    with pytest.raises(ValueError, match="Missing required parameter: workflowId"):
        handler(event, {})


@mock_dynamodb
def test_success_handler_minimal_event(setup_environment, workflow_table_name, job_status_table_name):
    # Set up mock DynamoDB tables
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    
    # Create workflow table
    workflow_table = dynamodb.create_table(
        TableName=workflow_table_name,
        KeySchema=[
            {'AttributeName': 'workflowId', 'KeyType': 'HASH'}
        ],
        AttributeDefinitions=[
            {'AttributeName': 'workflowId', 'AttributeType': 'S'}
        ],
        BillingMode='PAY_PER_REQUEST'
    )
    
    # Create job status table
    job_status_table = dynamodb.create_table(
        TableName=job_status_table_name,
        KeySchema=[
            {'AttributeName': 'workflowId', 'KeyType': 'HASH'},
            {'AttributeName': 'jobId', 'KeyType': 'RANGE'}
        ],
        AttributeDefinitions=[
            {'AttributeName': 'workflowId', 'AttributeType': 'S'},
            {'AttributeName': 'jobId', 'AttributeType': 'S'}
        ],
        BillingMode='PAY_PER_REQUEST'
    )
    
    # Create test workflow record
    workflow_id = "test-workflow-456"
    timestamp = datetime.utcnow().isoformat()
    workflow_table.put_item(
        Item={
            'workflowId': workflow_id,
            'createdAt': timestamp,
            'updatedAt': timestamp,
            'status': 'IN_PROGRESS'
        }
    )
    
    # Create minimal test event (just workflow ID)
    event = {
        'workflowId': workflow_id
    }
    
    # Call the handler
    result = handler(event, {})
    
    # Verify the result
    assert result['workflowId'] == workflow_id
    assert result['status'] == 'COMPLETED'
    assert result['jobStats']['total'] == 0  # No jobs
    assert result['resultsBucketPath'] is None
    assert result['message'] == 'Workflow completed successfully'
    
    # Verify workflow status was updated in DynamoDB
    workflow = workflow_table.get_item(
        Key={'workflowId': workflow_id}
    )['Item']
    assert workflow['status'] == 'COMPLETED'
    assert 'jobStats' in workflow
    assert 'resultsBucketPath' not in workflow  # Should not be added if not provided 