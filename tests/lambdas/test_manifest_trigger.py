import json
import os
import pytest
from unittest.mock import patch, MagicMock
import boto3
from moto import mock_aws
import importlib.util

# Import the functions from the Lambda
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '../../src/lambdas/manifest_trigger'))

spec = importlib.util.spec_from_file_location(
    "lambda_function",
    os.path.join(os.path.dirname(__file__), '../../src/lambdas/manifest_trigger/lambda_function.py')
)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

@pytest.fixture
def setup_environment():
    with patch.dict(os.environ, {
        'STATE_MACHINE_ARN': 'arn:aws:states:us-east-1:123456789012:stateMachine:GwasWorkflow',
        'NOTIFICATION_TOPIC_ARN': 'arn:aws:sns:us-east-1:123456789012:GwasNotifications'
    }):
        yield

# Unit tests for helper functions
def test_is_manifest_file():
    # Should match these patterns
    assert module.is_manifest_file("manifest.json") == True
    assert module.is_manifest_file("path/to/manifest.json") == True
    assert module.is_manifest_file("path/to/experiment.manifest.json") == True
    assert module.is_manifest_file("MANIFEST.JSON") == True  # Case insensitive
    
    # Should not match these
    assert module.is_manifest_file("manifest.txt") == False
    assert module.is_manifest_file("manifest_json") == False
    assert module.is_manifest_file("sample.manifest") == False

def test_ensure_trailing_slash():
    assert module.ensure_trailing_slash("s3://bucket/path") == "s3://bucket/path/"
    assert module.ensure_trailing_slash("s3://bucket/path/") == "s3://bucket/path/"

def test_parse_s3_uri():
    bucket, key = module.parse_s3_uri("s3://bucket/path/to/file.txt")
    assert bucket == "bucket"
    assert key == "path/to/file.txt"
    
    bucket, key = module.parse_s3_uri("not-an-s3-uri")
    assert bucket is None
    assert key is None

@mock_aws
def test_check_file_exists():
    # Set up mock S3
    s3 = boto3.client('s3')
    bucket_name = 'test-bucket'
    s3.create_bucket(Bucket=bucket_name)
    
    # Upload a test file
    s3.put_object(
        Bucket=bucket_name,
        Key='test-file.txt',
        Body='test content'
    )
    
    # Test
    assert module.check_file_exists(bucket_name, 'test-file.txt') == True
    assert module.check_file_exists(bucket_name, 'nonexistent-file.txt') == False

def test_validate_manifest_data(simple_manifest_data, complex_manifest_data):
    # Valid manifest should pass
    is_valid, error = module.validate_manifest_data(simple_manifest_data)
    assert is_valid == True
    assert error is None
    
    is_valid, error = module.validate_manifest_data(complex_manifest_data)
    assert is_valid == True
    assert error is None
    
    # Invalid manifest (missing required field)
    invalid_data = {"s3Path": "s3://bucket/path/"}
    is_valid, error = module.validate_manifest_data(invalid_data)
    assert is_valid == False
    assert error is not None
    
    # Invalid format
    invalid_format = simple_manifest_data.copy()
    invalid_format["inputData"] = {"format": "invalid", "filePrefix": "sample"}
    is_valid, error = module.validate_manifest_data(invalid_format)
    assert is_valid == False
    assert error is not None

@mock_aws
def test_validate_required_files_exist(simple_manifest_data):
    # Set up mock S3
    s3 = boto3.client('s3')
    bucket_name = 'test-bucket'
    s3.create_bucket(Bucket=bucket_name)
    
    # Create the required files for BED format
    for ext in ['bed', 'bim', 'fam']:
        s3.put_object(
            Bucket=bucket_name,
            Key=f'test-data/sample1.{ext}',
            Body=f'test {ext} content'
        )
    
    # Test with all files present
    is_valid, error = module.validate_required_files_exist(simple_manifest_data)
    assert is_valid == True
    assert error is None
    
    # Test with missing file
    s3.delete_object(Bucket=bucket_name, Key='test-data/sample1.bim')
    is_valid, error = module.validate_required_files_exist(simple_manifest_data)
    assert is_valid == False
    assert "Missing required files" in error

def test_prepare_workflow_input(simple_manifest_data, complex_manifest_data):
    # Test with simple manifest
    workflow_input, workflow_id = module.prepare_workflow_input(simple_manifest_data)
    assert workflow_id is not None
    assert workflow_input["s3Path"] == "s3://test-bucket/test-data/"
    assert workflow_input["inputData"]["format"] == "bed"
    assert workflow_input["outputParams"]["outputS3Path"] == "s3://test-bucket/test-data/results/"
    
    # Test with complex manifest
    workflow_input, workflow_id = module.prepare_workflow_input(complex_manifest_data)
    assert workflow_id is not None
    assert workflow_input["s3Path"] == "s3://test-bucket/test-data/"
    assert workflow_input["inputData"]["phenoFile"] == "pheno.txt"
    assert workflow_input["outputParams"]["outputS3Path"] == "s3://results-bucket/experiment1/"
    assert workflow_input["userId"] == "user123"
    assert workflow_input["studyId"] == "study456"

@mock_aws
def test_start_workflow():
    # Set up mock Step Functions
    sfn_client = boto3.client('stepfunctions', region_name='us-east-1')
    state_machine = sfn_client.create_state_machine(
        name='GwasWorkflow',
        definition='{"StartAt": "Test", "States": {"Test": {"Type": "Pass", "End": true}}}',       
        roleArn='arn:aws:iam::123456789012:role/test-role'
    )

    # Test starting workflow
    workflow_input = {"test": "data"}
    workflow_id = "test-workflow-123"
    success, result = module.start_workflow(state_machine['stateMachineArn'], workflow_input, workflow_id)

    assert success == True
    assert isinstance(result, str)  # Should be a string containing the execution ARN
    assert "execution:GwasWorkflow" in result  # Check for the execution part of the ARN

@mock_aws
def test_handle_manifest_event(s3_event, setup_environment, simple_manifest_data):
    # Set up mock S3
    s3 = boto3.client('s3')
    bucket_name = 'test-bucket'
    s3.create_bucket(Bucket=bucket_name)
    
    # Create the required files for BED format
    for ext in ['bed', 'bim', 'fam']:
        s3.put_object(
            Bucket=bucket_name,
            Key=f'test-data/sample1.{ext}',
            Body=f'test {ext} content'
        )
    
    # Upload a manifest file
    s3.put_object(
        Bucket=bucket_name,
        Key='test-data/manifest.json',
        Body=json.dumps(simple_manifest_data)
    )
    
    # Set up mock Step Functions
    sfn_client = boto3.client('stepfunctions', region_name='us-east-1')
    state_machine = sfn_client.create_state_machine(
        name='GwasWorkflow',
        definition='{"StartAt": "Test", "States": {"Test": {"Type": "Pass", "End": true}}}',
        roleArn='arn:aws:iam::123456789012:role/test-role'
    )
    
    # Patch send_notification in the correct module namespace
    with patch.object(module, 'send_notification') as mock_send_notification:
        mock_send_notification.return_value = True
        # Call the function
        result = module.handle_manifest_event(s3_event['Records'][0])
        # Assertions
        assert result['success'] == True
        assert 'workflowId' in result
        assert 'executionArn' in result
        assert mock_send_notification.called

@mock_aws
@mock_aws
@mock_aws
@mock_aws
def test_process_sqs_message(sqs_event, setup_environment, simple_manifest_data):
    # Set up mock S3
    s3 = boto3.client('s3')
    bucket_name = 'test-bucket'
    s3.create_bucket(Bucket=bucket_name)
    
    # Create the required files for BED format
    for ext in ['bed', 'bim', 'fam']:
        s3.put_object(
            Bucket=bucket_name,
            Key=f'test-data/sample1.{ext}',
            Body=f'test {ext} content'
        )
    
    # Upload a manifest file
    s3.put_object(
        Bucket=bucket_name,
        Key='test-data/manifest.json',
        Body=json.dumps(simple_manifest_data)
    )
    
    # Set up mock Step Functions
    sfn_client = boto3.client('stepfunctions', region_name='us-east-1')
    state_machine = sfn_client.create_state_machine(
        name='GwasWorkflow',
        definition='{"StartAt": "Test", "States": {"Test": {"Type": "Pass", "End": true}}}',
        roleArn='arn:aws:iam::123456789012:role/test-role'
    )
    
    # Set up SNS
    sns_client = boto3.client('sns', region_name='us-east-1')
    topic = sns_client.create_topic(Name='GwasNotifications')
    
    # Call the function with patched handle_manifest_event
    with patch.object(module, 'handle_manifest_event') as mock_handle:
        mock_handle.return_value = {
            'success': True, 
            'workflowId': 'test-workflow', 
            'executionArn': 'test-arn'
        }
        results = module.process_sqs_message(sqs_event['Records'][0])
    
    # Assertions
    assert len(results) > 0
    assert results[0]['success'] == True

@mock_aws
@mock_aws
@mock_aws
@mock_aws
def test_handler(sqs_event, setup_environment, simple_manifest_data):
    # Set up mock S3
    s3 = boto3.client('s3')
    bucket_name = 'test-bucket'
    s3.create_bucket(Bucket=bucket_name)
    
    # Create the required files for BED format
    for ext in ['bed', 'bim', 'fam']:
        s3.put_object(
            Bucket=bucket_name,
            Key=f'test-data/sample1.{ext}',
            Body=f'test {ext} content'
        )
    
    # Upload a manifest file
    s3.put_object(
        Bucket=bucket_name,
        Key='test-data/manifest.json',
        Body=json.dumps(simple_manifest_data)
    )
    
    # Set up mock Step Functions
    sfn_client = boto3.client('stepfunctions', region_name='us-east-1')
    state_machine = sfn_client.create_state_machine(
        name='GwasWorkflow',
        definition='{"StartAt": "Test", "States": {"Test": {"Type": "Pass", "End": true}}}',
        roleArn='arn:aws:iam::123456789012:role/test-role'
    )
    
    # Set up SNS
    sns_client = boto3.client('sns', region_name='us-east-1')
    topic = sns_client.create_topic(Name='GwasNotifications')
    
    # Call the handler with mocked process_sqs_message
    with patch.object(module, 'process_sqs_message') as mock_process:
        mock_process.return_value = [{
            'success': True, 
            'workflowId': 'test-workflow', 
            'executionArn': 'test-arn'
        }]
        result = module.handler(sqs_event, {})
    
    # Assertions
    assert result['statusCode'] == 200
    body = json.loads(result['body'])
    assert body['summary']['successCount'] == 1
    assert body['summary']['failureCount'] == 0

@mock_aws
def test_handler_missing_s3_file():
    event = {
        'Records': [
            {
                's3': {
                    'bucket': {'name': 'test-bucket'},
                    'object': {'key': 'nonexistent-file.txt'}
                }
            }
        ]
    }
    result = module.handler(event, {})
    assert result['statusCode'] == 200
    body = json.loads(result['body'])
    assert body['summary']['failureCount'] == 1
    assert "Error processing SQS message" in body['processingResults'][0]['error'] 

@mock_aws
def test_send_notification_error():
    # Test error handling in send_notification
    with patch.object(module, 'sns') as mock_sns:
        mock_sns.publish.side_effect = Exception("SNS error")
        result = module.send_notification("arn:aws:sns:region:account:topic", "Test", "Message")
        assert result == False

@mock_aws
def test_start_workflow_error():
    # Test error handling in start_workflow
    with patch.object(module, 'sfn') as mock_sfn:
        mock_sfn.start_execution.side_effect = Exception("Step Functions error")
        success, error = module.start_workflow("arn:aws:states:region:account:stateMachine:name", {}, "test-workflow")
        assert success == False
        assert "Step Functions error" in error

@mock_aws
def test_validate_required_files_pgen():
    # Test validation for PGEN format
    manifest_data = {
        "s3Path": "s3://test-bucket/test-data/",
        "inputData": {
            "format": "pgen",
            "filePrefix": "test"
        }
    }
    
    # Set up mock S3
    s3 = boto3.client('s3')
    s3.create_bucket(Bucket='test-bucket')
    
    # Create required PGEN files
    for ext in ['pgen', 'pvar', 'psam']:
        s3.put_object(
            Bucket='test-bucket',
            Key=f'test-data/test.{ext}',
            Body=f'test {ext} content'
        )
    
    is_valid, error = module.validate_required_files_exist(manifest_data)
    assert is_valid == True
    assert error is None

@mock_aws
def test_validate_required_files_bgen():
    # Test validation for BGEN format
    manifest_data = {
        "s3Path": "s3://test-bucket/test-data/",
        "inputData": {
            "format": "bgen",
            "filePrefix": "test"
        }
    }
    
    # Set up mock S3
    s3 = boto3.client('s3')
    s3.create_bucket(Bucket='test-bucket')
    
    # Create required BGEN files
    for ext in ['bgen', 'sample']:
        s3.put_object(
            Bucket='test-bucket',
            Key=f'test-data/test.{ext}',
            Body=f'test {ext} content'
        )
    
    is_valid, error = module.validate_required_files_exist(manifest_data)
    assert is_valid == True
    assert error is None

@mock_aws
def test_handle_manifest_event_missing_state_machine():
    # Test error handling when state machine ARN is missing
    # Set up mock S3
    s3 = boto3.client('s3')
    s3.create_bucket(Bucket='test-bucket')
    
    # Create manifest file
    manifest_data = {
        "experimentId": "test-experiment",
        "s3Path": "s3://test-bucket/test-data/",
        "inputData": {
            "format": "bed",
            "filePrefix": "test"
        }
    }
    s3.put_object(
        Bucket='test-bucket',
        Key='test-data/manifest.json',
        Body=json.dumps(manifest_data)
    )
    
    # Create required BED files
    for ext in ['bed', 'bim', 'fam']:
        s3.put_object(
            Bucket='test-bucket',
            Key=f'test-data/test.{ext}',
            Body=f'test {ext} content'
        )
    
    with patch.dict(os.environ, {}, clear=True):
        event = {
            's3': {
                'bucket': {'name': 'test-bucket'},
                'object': {'key': 'test-data/manifest.json'}
            }
        }
        result = module.handle_manifest_event(event)
        assert result['success'] == False
        assert "STATE_MACHINE_ARN environment variable not set" in result['error']

@mock_aws
def test_handle_manifest_event_notification_failure():
    # Test error handling when SNS notification fails
    with patch.dict(os.environ, {
        'STATE_MACHINE_ARN': 'arn:aws:states:region:account:stateMachine:name',
        'NOTIFICATION_TOPIC_ARN': 'arn:aws:sns:region:account:topic'
    }):
        # Set up mock S3
        s3 = boto3.client('s3')
        s3.create_bucket(Bucket='test-bucket')
        
        # Create manifest file
        manifest_data = {
            "experimentId": "test-experiment",
            "s3Path": "s3://test-bucket/test-data/",
            "inputData": {
                "format": "bed",
                "filePrefix": "test"
            }
        }
        s3.put_object(
            Bucket='test-bucket',
            Key='test-data/manifest.json',
            Body=json.dumps(manifest_data)
        )
        
        # Create required BED files
        for ext in ['bed', 'bim', 'fam']:
            s3.put_object(
                Bucket='test-bucket',
                Key=f'test-data/test.{ext}',
                Body=f'test {ext} content'
            )
        
        # Mock Step Functions success but SNS failure
        with patch.object(module, 'sfn') as mock_sfn, \
             patch.object(module, 'sns') as mock_sns:
            mock_sfn.start_execution.return_value = {'executionArn': 'test-arn'}
            mock_sns.publish.side_effect = Exception("SNS error")
            
            event = {
                's3': {
                    'bucket': {'name': 'test-bucket'},
                    'object': {'key': 'test-data/manifest.json'}
                }
            }
            result = module.handle_manifest_event(event)
            assert result['success'] == True  # Should still succeed even if notification fails
            assert 'workflowId' in result
            assert 'executionArn' in result 