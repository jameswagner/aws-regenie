import json
import os
import uuid
import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime
import boto3
from moto import mock_aws

# Import the handler from the Lambda function - try both options
import sys
sys.path.append('..')
try:
    from src.lambdas.workflow_init.lambda_function import handler
except ImportError:
    import importlib.util
    spec = importlib.util.spec_from_file_location("lambda_function", "../src/lambdas/workflow_init/lambda_function.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    handler = module.handler

@pytest.fixture
def workflow_table_name():
    return "test-workflow-table"

@pytest.fixture
def samples_table_name():
    return "test-samples-table"

@pytest.fixture
def study_metadata_table_name():
    return "test-study-metadata-table"

@pytest.fixture
def dataset_table_name():
    return "test-dataset-table"

@pytest.fixture
def setup_environment(workflow_table_name, samples_table_name, study_metadata_table_name, dataset_table_name):
    with patch.dict(os.environ, {
        'WORKFLOW_TABLE_NAME': workflow_table_name,
        'SAMPLES_TABLE_NAME': samples_table_name,
        'STUDY_METADATA_TABLE_NAME': study_metadata_table_name,
        'DATASET_TABLE_NAME': dataset_table_name,
        'DATA_BUCKET_NAME': 'test-data-bucket',
        'RESULTS_BUCKET_NAME': 'test-results-bucket',
    }):
        yield

@mock_aws
def test_handler_basic_execution(setup_environment, workflow_table_name, samples_table_name, study_metadata_table_name, dataset_table_name):
    # Set up mock DynamoDB tables
    dynamodb = boto3.resource('dynamodb')
    
    # Create workflow table
    dynamodb.create_table(
        TableName=workflow_table_name,
        KeySchema=[
            {'AttributeName': 'workflowId', 'KeyType': 'HASH'},
            {'AttributeName': 'createdAt', 'KeyType': 'RANGE'}
        ],
        AttributeDefinitions=[
            {'AttributeName': 'workflowId', 'AttributeType': 'S'},
            {'AttributeName': 'createdAt', 'AttributeType': 'S'}
        ],
        BillingMode='PAY_PER_REQUEST'
    )
    
    # Create samples table
    dynamodb.create_table(
        TableName=samples_table_name,
        KeySchema=[
            {'AttributeName': 'sampleId', 'KeyType': 'HASH'}
        ],
        AttributeDefinitions=[
            {'AttributeName': 'sampleId', 'AttributeType': 'S'}
        ],
        BillingMode='PAY_PER_REQUEST'
    )
    
    # Create study metadata table
    dynamodb.create_table(
        TableName=study_metadata_table_name,
        KeySchema=[
            {'AttributeName': 'studyId', 'KeyType': 'HASH'}
        ],
        AttributeDefinitions=[
            {'AttributeName': 'studyId', 'AttributeType': 'S'}
        ],
        BillingMode='PAY_PER_REQUEST'
    )
    
    # Create dataset table
    dynamodb.create_table(
        TableName=dataset_table_name,
        KeySchema=[
            {'AttributeName': 'datasetId', 'KeyType': 'HASH'}
        ],
        AttributeDefinitions=[
            {'AttributeName': 'datasetId', 'AttributeType': 'S'}
        ],
        BillingMode='PAY_PER_REQUEST'
    )
    
    # Create test study
    study_table = dynamodb.Table(study_metadata_table_name)
    study_id = "test-study-123"
    study_table.put_item(
        Item={
            'studyId': study_id,
            'studyName': 'Test Study',
            'principalInvestigator': 'Test PI',
            'studyStatus': 'SETUP',
            'createdAt': datetime.utcnow().isoformat()
        }
    )
    
    # Set up mock S3
    s3 = boto3.client('s3')
    bucket_name = 'test-data-bucket'
    s3.create_bucket(Bucket=bucket_name)
    
    # Upload a test file to simulate dataset existence
    s3.put_object(
        Bucket=bucket_name,
        Key='dataset1/testfile.txt',
        Body='test content'
    )
    
    # Create test event
    event = {
        "studyId": study_id,
        "sampleSize": 500,
        "batchSize": 50,
        "s3Path": f"s3://{bucket_name}/dataset1",
        "phenotype": "diabetes",
        "samples": [
            {
                "sampleId": "sample-001",
                "diabetes": "0",
                "gender": "F",
                "age": "45"
            },
            {
                "sampleId": "sample-002",
                "diabetes": "1", 
                "gender": "M",
                "age": "52"
            }
        ]
    }
    
    # Call the handler
    result = handler(event, {})
    
    # Assertions
    assert result['workflowId'] is not None
    assert result['status'] == 'INITIALIZED'
    assert result['datasetId'] is not None
    assert result['studyId'] == study_id
    assert result['processedSamples'] == 2  # Should match number of samples provided
    
    # Check that workflow was recorded in DynamoDB
    workflow_table = dynamodb.Table(workflow_table_name)
    workflows = workflow_table.scan()['Items']
    assert len(workflows) == 1
    
    # Check that samples were recorded in DynamoDB
    samples_table = dynamodb.Table(samples_table_name)
    samples = samples_table.scan()['Items']
    assert len(samples) == 2
    
    # Check that study status was updated
    study = study_table.get_item(Key={'studyId': study_id})['Item']
    assert study['studyStatus'] == 'WORKFLOW_RUNNING'

@mock_aws
def test_handler_with_sample_manifest(setup_environment, workflow_table_name, samples_table_name, dataset_table_name):
    # Set up mock DynamoDB tables
    dynamodb = boto3.resource('dynamodb')
    
    # Create required tables
    dynamodb.create_table(
        TableName=workflow_table_name,
        KeySchema=[
            {'AttributeName': 'workflowId', 'KeyType': 'HASH'},
            {'AttributeName': 'createdAt', 'KeyType': 'RANGE'}
        ],
        AttributeDefinitions=[
            {'AttributeName': 'workflowId', 'AttributeType': 'S'},
            {'AttributeName': 'createdAt', 'AttributeType': 'S'}
        ],
        BillingMode='PAY_PER_REQUEST'
    )
    
    dynamodb.create_table(
        TableName=samples_table_name,
        KeySchema=[
            {'AttributeName': 'sampleId', 'KeyType': 'HASH'}
        ],
        AttributeDefinitions=[
            {'AttributeName': 'sampleId', 'AttributeType': 'S'}
        ],
        BillingMode='PAY_PER_REQUEST'
    )
    
    dynamodb.create_table(
        TableName=dataset_table_name,
        KeySchema=[
            {'AttributeName': 'datasetId', 'KeyType': 'HASH'}
        ],
        AttributeDefinitions=[
            {'AttributeName': 'datasetId', 'AttributeType': 'S'}
        ],
        BillingMode='PAY_PER_REQUEST'
    )
    
    # Set up mock S3
    s3 = boto3.client('s3')
    bucket_name = 'test-data-bucket'
    s3.create_bucket(Bucket=bucket_name)
    
    # Upload a test file to simulate dataset existence
    s3.put_object(
        Bucket=bucket_name,
        Key='dataset1/testfile.txt',
        Body='test content'
    )
    
    # Upload a sample manifest file
    sample_csv = "sampleId,diabetes,gender,age\nsample-001,0,F,45\nsample-002,1,M,52\nsample-003,0,F,38"
    s3.put_object(
        Bucket=bucket_name,
        Key='manifests/sample-manifest.csv',
        Body=sample_csv
    )
    
    # Create test event with sample manifest
    event = {
        "sampleSize": 500,
        "batchSize": 50,
        "s3Path": f"s3://{bucket_name}/dataset1",
        "phenotype": "diabetes",
        "sampleManifest": f"s3://{bucket_name}/manifests/sample-manifest.csv"
    }
    
    # Call the handler
    result = handler(event, {})
    
    # Assertions
    assert result['workflowId'] is not None
    assert result['status'] == 'INITIALIZED'
    assert result['datasetId'] is not None
    assert result['processedSamples'] == 0  # No samples provided directly
    
    # Check that workflow was recorded in DynamoDB
    workflow_table = dynamodb.Table(workflow_table_name)
    workflows = workflow_table.scan()['Items']
    assert len(workflows) == 1

@mock_aws
def test_handler_with_existing_dataset(setup_environment, workflow_table_name, samples_table_name, dataset_table_name):
    # Set up mock DynamoDB tables
    dynamodb = boto3.resource('dynamodb')
    
    # Create required tables
    dynamodb.create_table(
        TableName=workflow_table_name,
        KeySchema=[
            {'AttributeName': 'workflowId', 'KeyType': 'HASH'},
            {'AttributeName': 'createdAt', 'KeyType': 'RANGE'}
        ],
        AttributeDefinitions=[
            {'AttributeName': 'workflowId', 'AttributeType': 'S'},
            {'AttributeName': 'createdAt', 'AttributeType': 'S'}
        ],
        BillingMode='PAY_PER_REQUEST'
    )
    
    dynamodb.create_table(
        TableName=samples_table_name,
        KeySchema=[
            {'AttributeName': 'sampleId', 'KeyType': 'HASH'}
        ],
        AttributeDefinitions=[
            {'AttributeName': 'sampleId', 'AttributeType': 'S'}
        ],
        BillingMode='PAY_PER_REQUEST'
    )
    
    # Create dataset table
    dataset_table = dynamodb.create_table(
        TableName=dataset_table_name,
        KeySchema=[
            {'AttributeName': 'datasetId', 'KeyType': 'HASH'}
        ],
        AttributeDefinitions=[
            {'AttributeName': 'datasetId', 'AttributeType': 'S'}
        ],
        BillingMode='PAY_PER_REQUEST'
    )
    
    # Create existing dataset
    dataset_id = "existing-dataset-123"
    dataset_table.put_item(
        Item={
            'datasetId': dataset_id,
            'createdAt': datetime.utcnow().isoformat(),
            'updatedAt': datetime.utcnow().isoformat(),
            'dataPath': 's3://test-data-bucket/dataset1',
            'bucketName': 'test-data-bucket',
            'bucketPrefix': 'dataset1',
            'dataType': 'GWAS',
            'lastUsed': datetime.utcnow().isoformat(),
            'sampleCount': 1000
        }
    )
    
    # Set up mock S3
    s3 = boto3.client('s3')
    bucket_name = 'test-data-bucket'
    s3.create_bucket(Bucket=bucket_name)
    
    # Upload a test file to simulate dataset existence
    s3.put_object(
        Bucket=bucket_name,
        Key='dataset1/testfile.txt',
        Body='test content'
    )
    
    # Create test event with existing dataset
    event = {
        "sampleSize": 500,
        "batchSize": 50,
        "datasetId": dataset_id,
        "s3Path": f"s3://{bucket_name}/dataset1",
        "phenotype": "diabetes"
    }
    
    # Call the handler
    result = handler(event, {})
    
    # Assertions
    assert result['workflowId'] is not None
    assert result['status'] == 'INITIALIZED'
    assert result['datasetId'] == dataset_id  # Should use the existing dataset ID
    
    # Check that dataset was updated in DynamoDB
    updated_dataset = dataset_table.get_item(Key={'datasetId': dataset_id})['Item']
    assert 'lastUsed' in updated_dataset
    assert 'updatedAt' in updated_dataset 