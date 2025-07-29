import json
import os
import pytest
from unittest.mock import patch, MagicMock
import boto3
from moto import mock_aws

# Import handler directly using file path
import sys
import importlib.util

# Use the correct path - no leading dots
spec = importlib.util.spec_from_file_location("lambda_function", "src/lambdas/command_parser/lambda_function.py")
module = importlib.util.module_from_spec(spec)
sys.modules["lambda_function"] = module
spec.loader.exec_module(module)
handler = module.handler

@pytest.fixture
def setup_environment():
    with patch.dict(os.environ, {
        'JOB_DEFINITION_ARN': 'arn:aws:batch:us-east-1:123456789012:job-definition/regenie:1',
    }):
        yield

def test_handler_parse_step1_command(setup_environment):
    # Create test event for Step 1 command
    event = {
        'JobId': 'job-123',
        'Command': './regenie --step 1 --bed example/example --exclude example/snplist_rm.txt --covarFile example/covariates.txt --phenoFile example/phenotype_bin.txt --bsize 100 --bt --out fit_bin_out',
        'StartStep': '1'
    }

    # Call the handler
    result = handler(event, {})

    # Assertions
    assert 'jobSubmission' in result
    assert 'ContainerOverrides' in result['jobSubmission']
    assert 'Command' in result['jobSubmission']['ContainerOverrides']

    # Check that the command was properly wrapped in shell
    command = result['jobSubmission']['ContainerOverrides']['Command']
    assert isinstance(command, list)
    assert command[0] == '/bin/bash'
    assert command[1] == '-c'
    assert command[2] == event['Command']

def test_handler_parse_step2_command(setup_environment):
    # Create test event for Step 2 command
    event = {
        'JobId': 'job-456',
        'Command': './regenie --step 2 --bgen example/example.bgen --covarFile example/covariates.txt --phenoFile example/phenotype_bin.txt --bsize 200 --bt --pred fit_bin_out_pred.list --out test_bin_out',
        'StartStep': '1'
    }

    # Call the handler
    result = handler(event, {})

    # Assertions
    assert 'jobSubmission' in result
    assert 'ContainerOverrides' in result['jobSubmission']
    assert 'Command' in result['jobSubmission']['ContainerOverrides']

    # Check that the command was properly wrapped in shell
    command = result['jobSubmission']['ContainerOverrides']['Command']
    assert isinstance(command, list)
    assert command[0] == '/bin/bash'
    assert command[1] == '-c'
    assert command[2] == event['Command']

def test_handler_with_custom_command(setup_environment):
    # Create test event with a custom command
    event = {
        'JobId': 'job-789',
        'Command': './regenie --custom-param --step 2 --bgen custom/path --phenoFile custom/phenotype.txt',
        'StartStep': '1'
    }

    # Call the handler
    result = handler(event, {})

    # Assertions
    assert 'jobSubmission' in result
    assert 'ContainerOverrides' in result['jobSubmission']
    assert 'Command' in result['jobSubmission']['ContainerOverrides']

    # Check that the custom command was properly wrapped in shell
    command = result['jobSubmission']['ContainerOverrides']['Command']
    assert isinstance(command, list)
    assert command[0] == '/bin/bash'
    assert command[1] == '-c'
    assert command[2] == event['Command']

def test_handler_when_start_with_step2(setup_environment):
    # Create test event with StartStep set to '2'
    event = {
        'JobId': 'job-101112',
        'Command': './regenie --step 2 --bgen example/example.bgen --covarFile example/covariates.txt --phenoFile example/phenotype_bin.txt --bsize 200 --bt --pred fit_bin_out_pred.list --out test_bin_out',
        'StartStep': '2'
    }

    # Call the handler
    result = handler(event, {})

    # Assertions
    assert 'jobSubmission' in result
    assert 'ContainerOverrides' in result['jobSubmission']
    assert 'Command' in result['jobSubmission']['ContainerOverrides']

    # Check that the command was properly wrapped in shell
    command = result['jobSubmission']['ContainerOverrides']['Command']
    assert isinstance(command, list)
    assert command[0] == '/bin/bash'
    assert command[1] == '-c'
    assert command[2] == event['Command']

def test_handler_with_version_override(setup_environment):
    # Create test event with version override
    event = {
        'JobId': 'job-131415',
        'Command': './regenie --version 2.2.4 --step 1 --bed example/example --exclude example/snplist_rm.txt --covarFile example/covariates.txt --phenoFile example/phenotype_bin.txt --bsize 100 --bt --out fit_bin_out',
        'StartStep': '1'
    }

    # Call the handler
    result = handler(event, {})

    # Assertions
    assert 'jobSubmission' in result
    assert 'ContainerOverrides' in result['jobSubmission']
    assert 'Command' in result['jobSubmission']['ContainerOverrides']

    # Check that the command was properly wrapped in shell
    command = result['jobSubmission']['ContainerOverrides']['Command']
    assert isinstance(command, list)
    assert command[0] == '/bin/bash'
    assert command[1] == '-c'
    assert command[2] == event['Command']

def test_handler_raises_exception_on_missing_command():
    event = {
        'JobId': 'job-123',
        # 'Command' is missing!
        'JobQueue': 'job-queue'
    }
    with pytest.raises(Exception):
        handler(event, {}) 

def test_handler_step2_with_fsx_and_chromosome():
    event = {
        'JobId': 'job-123',
        'Command': './regenie --step 2 --bgen example/example.bgen --covarFile example/covariates.txt --phenoFile example/phenotype_bin.txt --bsize 200 --bt --pred fit_bin_out_pred.list --out test_bin_out',
        'JobQueue': 'job-queue',
        'StartStep': '2',
        'StepNumber': 2,
        'UseFsx': True,
        'FsxPath': '/path/on/fsx',
        'ChromosomeNumber': '1',
        'PredListPath': '/path/to/pred.list',
        'OutputPrefix': 'output-prefix'
    }
    result = handler(event, {})
    assert 'jobSubmission' in result
    assert 'additionalInfo' in result
    additional_info = result['additionalInfo']
    assert additional_info['UseFsx'] is True
    assert additional_info['FsxPath'] == '/path/on/fsx'
    assert additional_info['Chromosome'] == '1'
    assert additional_info['PredListPath'] == '/path/to/pred.list'
    assert additional_info['OutputPrefix'] == 'output-prefix' 