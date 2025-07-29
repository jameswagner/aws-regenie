import json
import os
import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime
import boto3
from moto import mock_aws

# Import handler directly using file path
import sys
import importlib.util

# Use the correct path - no leading dots
spec = importlib.util.spec_from_file_location("lambda_function", "src/lambdas/job_calculator/lambda_function.py")
module = importlib.util.module_from_spec(spec)
sys.modules["lambda_function"] = module
spec.loader.exec_module(module)
handler = module.handler
get_chromosomes_from_bim = module.get_chromosomes_from_bim

# Sample BIM file content for testing
SAMPLE_BIM_CONTENT = """1	rs1001	0	1000	A	C
1	rs1002	0	2000	G	T
2	rs2001	0	1000	C	G
2	rs2002	0	2000	T	A
3	rs3001	0	1000	A	G
X	rsX001	0	1000	C	T
Y	rsY001	0	1000	G	C
MT	rsMT001	0	1000	A	T
"""

@pytest.fixture
def workflow_table_name():
    return "test-workflow-table"

@pytest.fixture
def job_status_table_name():
    return "test-job-status-table"

@pytest.fixture
def setup_environment(workflow_table_name, job_status_table_name):
    # Mock AWS config file
    with patch('botocore.configloader.raw_config_parse') as mock_config:
        mock_config.return_value = {'default': {}}
        with patch.dict(os.environ, {
            'WORKFLOW_TABLE_NAME': workflow_table_name,
            'JOB_STATUS_TABLE_NAME': job_status_table_name,
            'JOB_QUEUE_ARN': 'arn:aws:batch:us-east-1:123456789012:job-queue/test-queue',
            'AWS_DEFAULT_REGION': 'us-east-1'  # Add default region
        }):
            yield

@pytest.fixture
def mock_s3_bim_file():
    with mock_aws():
        s3 = boto3.client('s3', region_name='us-east-1')
        s3.create_bucket(Bucket='test-bucket')
        s3.put_object(
            Bucket='test-bucket',
            Key='test-dataset/test.bim',
            Body=SAMPLE_BIM_CONTENT
        )
        yield

@mock_aws
def test_get_chromosomes_from_bim(mock_s3_bim_file):
    # Test the function that extracts chromosomes from BIM file
    chromosomes = get_chromosomes_from_bim('s3://test-bucket/test-dataset/', 'test', 'bed')
    
    # Verify the expected chromosomes are extracted and sorted correctly
    assert chromosomes == ['1', '2', '3', 'MT', 'X', 'Y']

@mock_aws
def test_get_chromosomes_from_bim_unsupported_format():
    chromosomes = module.get_chromosomes_from_bim('s3://bucket/', 'prefix', 'bgen')
    assert chromosomes == module.DEFAULT_CHROMOSOMES

@mock_aws
def test_get_chromosomes_from_bim_s3_error():
    # Test error handling when S3 file doesn't exist
    chromosomes = get_chromosomes_from_bim('s3://nonexistent-bucket/path/', 'test', 'bed')
    assert chromosomes == module.DEFAULT_CHROMOSOMES

@mock_aws
def test_get_chromosomes_from_bim_parse_error():
    # Test error handling when BIM file parsing fails
    with mock_aws():
        s3 = boto3.client('s3', region_name='us-east-1')
        s3.create_bucket(Bucket='test-bucket')
        # Upload malformed BIM file (missing required fields)
        s3.put_object(
            Bucket='test-bucket',
            Key='test-dataset/test.bim',
            Body='X\nY\nMT\n'  # Just chromosome names without other required fields
        )
        chromosomes = get_chromosomes_from_bim('s3://test-bucket/test-dataset/', 'test', 'bed')
        assert chromosomes == module.DEFAULT_CHROMOSOMES

@mock_aws
def test_get_chromosomes_from_bim_temp_file_error():
    # Test error handling when temp file operations fail
    with patch('tempfile.NamedTemporaryFile', side_effect=Exception("Temp file error")):
        chromosomes = get_chromosomes_from_bim('s3://test-bucket/test-dataset/', 'test', 'bed')
        assert chromosomes == module.DEFAULT_CHROMOSOMES

@mock_aws
def test_get_chromosomes_from_bim_s3_download_error():
    # Test error handling when S3 download fails
    with mock_aws():
        s3 = boto3.client('s3', region_name='us-east-1')
        s3.create_bucket(Bucket='test-bucket')
        with patch.object(s3, 'download_file', side_effect=Exception("S3 download error")):
            chromosomes = get_chromosomes_from_bim('s3://test-bucket/test-dataset/', 'test', 'bed')
            assert chromosomes == module.DEFAULT_CHROMOSOMES

@mock_aws
def test_get_chromosomes_from_bim_file_parse_error():
    # Test error handling when file parsing fails
    with mock_aws():
        s3 = boto3.client('s3', region_name='us-east-1')
        s3.create_bucket(Bucket='test-bucket')
        s3.put_object(
            Bucket='test-bucket',
            Key='test-dataset/test.bim',
            Body='invalid\nformat\n'  # Malformed BIM file
        )
        chromosomes = get_chromosomes_from_bim('s3://test-bucket/test-dataset/', 'test', 'bed')
        assert chromosomes == module.DEFAULT_CHROMOSOMES

@mock_aws
def test_handler_basic_execution(setup_environment, workflow_table_name, job_status_table_name, mock_s3_bim_file):
    # Set up mock DynamoDB tables
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    
    # Create workflow table with only workflowId as partition key
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
    dynamodb.create_table(
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
            'updatedAt': timestamp,
            'status': 'INITIALIZED',
            'parameters': {
                'sampleSize': 1000,
                'batchSize': 100,
                'regenieVersion': 'latest',
                'startStep': '1',
                'phenotype': 'diabetes'
            }
        }
    )
    
    # Create test event
    event = {
        'workflowId': workflow_id,
        'timestamp': timestamp,
        'status': 'INITIALIZED',
        'datasetPath': 's3://test-data-bucket/dataset1',
        'resultsBucketPath': 's3://test-results-bucket/workflows/test-workflow-123/',
        'startStep': 1,  # Move startStep to top level
        'parameters': {
            'sampleSize': 1000,
            'batchSize': 100,
            'regenieVersion': 'latest',
            'phenotype': 'diabetes'
        }
    }
    
    # Call the handler
    result = handler(event, {})
    
    # Assertions
    assert 'workflowId' in result
    assert 'startStep' in result
    assert 'jobCount' in result
    assert 'step1Jobs' in result
    assert 'step2Jobs' in result
    assert 'chromosomes' in result
    
    # Check that the first job is step 1
    assert len(result['step1Jobs']) == 1
    assert result['step1Jobs'][0]['stepNumber'] == 1
    
    # Check that subsequent jobs are step 2 with chromosomes
    assert len(result['step2Jobs']) > 0
    for job in result['step2Jobs']:
        assert job['stepNumber'] == 2
        assert 'chromosome' in job['parameters']
        assert job['jobId'].startswith(f"{workflow_id}-step2-chr")

@mock_aws
def test_handler_with_start_step1(setup_environment, workflow_table_name, job_status_table_name):
    # Set up mock DynamoDB tables
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    
    # Create workflow table with only workflowId as partition key
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
    dynamodb.create_table(
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
            'updatedAt': timestamp,
            'status': 'INITIALIZED',
            'parameters': {
                'sampleSize': 1000,
                'batchSize': 100,
                'regenieVersion': 'latest',
                'startStep': '1',
                'phenotype': 'diabetes'
            }
        }
    )
    
    # Create test event
    event = {
        'workflowId': workflow_id,
        'timestamp': timestamp,
        'status': 'INITIALIZED',
        'datasetPath': 's3://test-data-bucket/dataset1',
        'resultsBucketPath': 's3://test-results-bucket/workflows/test-workflow-123/',
        'startStep': 1,  # Move startStep to top level
        'parameters': {
            'sampleSize': 1000,
            'batchSize': 100,
            'regenieVersion': 'latest',
            'phenotype': 'diabetes'
        }
    }
    
    # Call the handler
    result = handler(event, {})
    
    # Assertions
    assert 'workflowId' in result
    assert 'startStep' in result
    assert result['startStep'] == 1
    assert 'step1Jobs' in result
    assert len(result['step1Jobs']) == 1
    assert result['step1Jobs'][0]['stepNumber'] == 1
    assert 'step2Jobs' in result
    assert len(result['step2Jobs']) > 0

@mock_aws
def test_handler_with_start_step2(setup_environment, workflow_table_name, job_status_table_name):
    # Set up mock DynamoDB tables
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    
    # Create workflow table with only workflowId as partition key
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
    dynamodb.create_table(
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
            'updatedAt': timestamp,
            'status': 'INITIALIZED',
            'parameters': {
                'sampleSize': 1000,
                'batchSize': 100,
                'regenieVersion': 'latest',
                'startStep': '2',  # Start with step 2
                'phenotype': 'diabetes'
            }
        }
    )
    
    # Create test event
    event = {
        'workflowId': workflow_id,
        'timestamp': timestamp,
        'status': 'INITIALIZED',
        'datasetPath': 's3://test-data-bucket/dataset1',
        'resultsBucketPath': 's3://test-results-bucket/workflows/test-workflow-123/',
        'startStep': 2,  # Move startStep to top level
        'predictionFile': '/gwas-experiments/results_pred.list',  # Add prediction file for step 2
        'parameters': {
            'sampleSize': 1000,
            'batchSize': 100,
            'regenieVersion': 'latest',
            'phenotype': 'diabetes'
        }
    }
    
    # Call the handler
    result = handler(event, {})
    
    # Assertions
    assert 'workflowId' in result
    assert 'startStep' in result
    assert result['startStep'] == 2
    assert 'step1Jobs' in result
    assert len(result['step1Jobs']) == 0  # No step 1 jobs when starting at step 2
    assert 'step2Jobs' in result
    assert len(result['step2Jobs']) > 0

@mock_aws
def test_handler_with_custom_regenie_version(setup_environment, workflow_table_name, job_status_table_name):
    # Set up mock DynamoDB tables
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    
    # Create workflow table with only workflowId as partition key
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
    dynamodb.create_table(
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
            'updatedAt': timestamp,
            'status': 'INITIALIZED',
            'parameters': {
                'sampleSize': 1000,
                'batchSize': 100,
                'regenieVersion': '3.2.1',  # Custom version
                'startStep': '1',
                'phenotype': 'diabetes'
            }
        }
    )
    
    # Create test event
    event = {
        'workflowId': workflow_id,
        'timestamp': timestamp,
        'status': 'INITIALIZED',
        'datasetPath': 's3://test-data-bucket/dataset1',
        'resultsBucketPath': 's3://test-results-bucket/workflows/test-workflow-123/',
        'startStep': 1,  # Move startStep to top level
        'parameters': {
            'sampleSize': 1000,
            'batchSize': 100,
            'regenieVersion': '3.2.1',  # Custom version
            'phenotype': 'diabetes'
        }
    }
    
    # Call the handler
    result = handler(event, {})
    
    # Assertions
    assert 'workflowId' in result
    assert 'startStep' in result
    assert 'step1Jobs' in result
    assert len(result['step1Jobs']) == 1
    assert result['step1Jobs'][0]['stepNumber'] == 1
    assert 'step2Jobs' in result
    assert len(result['step2Jobs']) > 0

@mock_aws
def test_handler_missing_workflow_id(setup_environment, workflow_table_name, job_status_table_name):
    # Create test event with missing workflowId
    event = {
        'timestamp': datetime.utcnow().isoformat(),
        'status': 'INITIALIZED',
        'datasetPath': 's3://test-data-bucket/dataset1',
        'resultsBucketPath': 's3://test-results-bucket/workflows/test-workflow-123/',
        'startStep': 1,
        'parameters': {
            'sampleSize': 1000,
            'batchSize': 100,
            'regenieVersion': 'latest',
            'phenotype': 'diabetes'
        }
    }
    with pytest.raises(ValueError, match="Missing required parameter: workflowId"):
        handler(event, {})

@mock_aws
def test_handler_step2_missing_prediction_file(setup_environment, workflow_table_name, job_status_table_name):
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
    dynamodb.create_table(
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

    event = {
        'workflowId': 'test-workflow-123',
        'timestamp': datetime.utcnow().isoformat(),
        'status': 'INITIALIZED',
        'datasetPath': 's3://test-data-bucket/dataset1',
        'resultsBucketPath': 's3://test-results-bucket/workflows/test-workflow-123/',
        'startStep': 2,
        'parameters': {
            'sampleSize': 1000,
            'batchSize': 100,
            'regenieVersion': 'latest',
            'phenotype': 'diabetes'
        }
    }
    with pytest.raises(ValueError, match="When starting with step 2, a prediction file must be provided"):
        handler(event, {})

@mock_aws
def test_handler_dynamodb_error(setup_environment, workflow_table_name, job_status_table_name):
    # Test error handling when DynamoDB operations fail
    event = {
        'workflowId': 'test-workflow-123',
        'timestamp': datetime.utcnow().isoformat(),
        'status': 'INITIALIZED',
        'datasetPath': 's3://test-data-bucket/dataset1',
        'resultsBucketPath': 's3://test-results-bucket/workflows/test-workflow-123/',
        'startStep': 1,
        'parameters': {
            'sampleSize': 1000,
            'batchSize': 100,
            'regenieVersion': 'latest',
            'phenotype': 'diabetes'
        }
    }
    # Don't create the DynamoDB tables to force an error
    with pytest.raises(Exception):
        handler(event, {})

@mock_aws
def test_handler_pgen_format(setup_environment, workflow_table_name, job_status_table_name):
    # Test with PGEN format
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
    dynamodb.create_table(
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

    event = {
        'workflowId': 'test-workflow-123',
        'timestamp': datetime.utcnow().isoformat(),
        'status': 'INITIALIZED',
        'datasetPath': 's3://test-data-bucket/dataset1',
        'resultsBucketPath': 's3://test-results-bucket/workflows/test-workflow-123/',
        'startStep': 1,
        'parameters': {
            'sampleSize': 1000,
            'batchSize': 100,
            'regenieVersion': 'latest',
            'phenotype': 'diabetes'
        },
        'inputData': {
            'format': 'pgen',
            'filePrefix': 'test'
        }
    }
    result = handler(event, {})
    assert result['workflowId'] == 'test-workflow-123'
    assert len(result['step2Jobs']) > 0
    assert all(job['parameters']['dataFormat'] == 'pgen' for job in result['step2Jobs']) 