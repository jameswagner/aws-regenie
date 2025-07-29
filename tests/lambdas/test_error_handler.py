import json
import os
import uuid
import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime
import boto3
from moto import mock_aws

# Import handler directly using file path
import sys
import importlib.util

# Use the correct path - no leading dots
spec = importlib.util.spec_from_file_location("lambda_function", "src/lambdas/error_handler/lambda_function.py")
module = importlib.util.module_from_spec(spec)
sys.modules["lambda_function"] = module
spec.loader.exec_module(module)
handler = module.handler

@pytest.fixture(scope='function')
def setup_environment():
    """Set up environment variables for testing."""
    os.environ['WORKFLOW_TABLE_NAME'] = 'test-workflow-table'
    os.environ['JOB_STATUS_TABLE_NAME'] = 'test-job-status-table'
    yield
    # Clean up environment variables after test
    for key in ['WORKFLOW_TABLE_NAME', 'JOB_STATUS_TABLE_NAME']:
        os.environ.pop(key, None)

@pytest.fixture
def workflow_table_name():
    return 'test-workflow-table'

@pytest.fixture
def job_status_table_name():
    return 'test-job-status-table'

@mock_aws
def test_handler_with_failed_jobs(setup_environment, workflow_table_name, job_status_table_name):
    # Set up mock DynamoDB tables
    dynamodb = boto3.resource('dynamodb')
    
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
            'status': 'RUNNING'
        }
    )
    
    # Create test job records
    job_id_1 = "job-1"
    job_id_2 = "job-2"
    
    job_status_table.put_item(
        Item={
            'workflowId': workflow_id,
            'jobId': job_id_1,
            'status': 'RUNNING',
            'updatedAt': timestamp,
            'stepNumber': 1
        }
    )
    
    job_status_table.put_item(
        Item={
            'workflowId': workflow_id,
            'jobId': job_id_2,
            'status': 'RUNNING',
            'updatedAt': timestamp,
            'stepNumber': 2
        }
    )
    
    # Create test event with failed jobs
    event = {
        'workflowId': workflow_id,
        'failedJobs': [
            {
                'jobId': job_id_1,
                'errorMessage': 'Container error: Out of memory',
                'batchJobId': 'batch-job-1',
                'exitCode': 137
            },
            {
                'jobId': job_id_2,
                'errorMessage': 'Container error: Process exited with code 1',
                'batchJobId': 'batch-job-2',
                'exitCode': 1
            }
        ]
    }
    
    # Call the handler
    result = handler(event, {})
    
    # Verify the result
    assert result['workflowId'] == workflow_id
    assert result['status'] == 'FAILED'  # Because step 1 job failed
    assert result['processedErrors'] == 2
    assert result['jobStats']['total'] == 2
    assert result['jobStats']['failed'] == 2
    assert result['jobStats']['completed'] == 0
    assert result['jobStats']['pending'] == 0

    # Verify job status updates
    job1 = job_status_table.get_item(
        Key={'workflowId': workflow_id, 'jobId': job_id_1}
    )['Item']
    assert job1['status'] == 'FAILED'
    assert 'errorDetail' in job1

    job2 = job_status_table.get_item(
        Key={'workflowId': workflow_id, 'jobId': job_id_2}
    )['Item']
    assert job2['status'] == 'FAILED'
    assert 'errorDetail' in job2

    # Verify workflow status update
    workflow = workflow_table.get_item(
        Key={'workflowId': workflow_id}
    )['Item']
    assert workflow['status'] == 'FAILED'
    assert 'jobStats' in workflow

@mock_aws
def test_handler_with_no_failed_jobs(setup_environment, workflow_table_name, job_status_table_name):
    # Set up mock DynamoDB tables
    dynamodb = boto3.resource('dynamodb')
    
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
    
    # Create test event with no failed jobs
    event = {
        'workflowId': 'test-workflow-123',
        'failedJobs': []
    }
    
    # Call the handler
    result = handler(event, {})
    
    # Verify the result
    assert result['status'] == 'NO_FAILURES'

@mock_aws
def test_handler_with_specific_error_types(setup_environment, workflow_table_name, job_status_table_name):
    # Set up mock DynamoDB tables
    dynamodb = boto3.resource('dynamodb')
    
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
            'status': 'RUNNING'
        }
    )
    
    # Create test job records with different error types
    job_id_1 = "job-memory-error"
    job_id_2 = "job-regenie-error"
    
    job_status_table.put_item(
        Item={
            'workflowId': workflow_id,
            'jobId': job_id_1,
            'status': 'RUNNING',
            'updatedAt': timestamp,
            'stepNumber': 1
        }
    )
    
    job_status_table.put_item(
        Item={
            'workflowId': workflow_id,
            'jobId': job_id_2,
            'status': 'RUNNING',
            'updatedAt': timestamp,
            'stepNumber': 2
        }
    )
    
    # Create test event with specific error types
    event = {
        'workflowId': workflow_id,
        'failedJobs': [
            {
                'jobId': job_id_1,
                'errorMessage': 'Container error: Out of memory',
                'batchJobId': 'batch-job-1',
                'exitCode': 137
            },
            {
                'jobId': job_id_2,
                'errorMessage': 'Regenie error: Invalid parameter',
                'batchJobId': 'batch-job-2',
                'exitCode': 1
            }
        ]
    }
    
    # Call the handler
    result = handler(event, {})
    
    # Verify the result
    assert result['workflowId'] == workflow_id
    assert result['status'] == 'FAILED'  # Because step 1 job failed
    assert result['processedErrors'] == 2
    assert result['jobStats']['total'] == 2
    assert result['jobStats']['failed'] == 2
    assert result['jobStats']['completed'] == 0
    assert result['jobStats']['pending'] == 0

    # Verify job status updates
    job1 = job_status_table.get_item(
        Key={'workflowId': workflow_id, 'jobId': job_id_1}
    )['Item']
    assert job1['status'] == 'FAILED'
    assert 'errorDetail' in job1
    assert 'Out of memory' in job1['errorDetail']

    job2 = job_status_table.get_item(
        Key={'workflowId': workflow_id, 'jobId': job_id_2}
    )['Item']
    assert job2['status'] == 'FAILED'
    assert 'errorDetail' in job2
    assert 'Invalid parameter' in job2['errorDetail']

    # Verify workflow status update
    workflow = workflow_table.get_item(
        Key={'workflowId': workflow_id}
    )['Item']
    assert workflow['status'] == 'FAILED'
    assert 'jobStats' in workflow 