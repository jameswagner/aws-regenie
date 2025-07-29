import os
import sys
import pytest
from datetime import datetime
import boto3
from moto import mock_aws
from unittest.mock import patch
import json

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

# Add the project root directory to the Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

# Add src directory to Python path to allow importing from lambda modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src/lambdas')))

# Import pytest fixtures that can be reused across tests
@pytest.fixture
def mock_aws():
    """Configure moto properly for AWS mocking in tests."""
    # Import moto with proper configuration for AWS mocking
    try:
        # For moto v4+
        from moto import mock_aws
        
        with mock_aws():
            yield
    except ImportError:
        # If moto is older version, provide backward compatibility
        import pytest
        pytest.skip("Requires moto v4+ with mock_aws support")

@pytest.fixture(scope="function")
def mock_env_vars():
    """Set standard environment variables for testing."""
    original_env = dict(os.environ)
    
    # Set test environment variables
    os.environ.update({
        'WORKFLOW_TABLE_NAME': 'test-workflow-table',
        'JOB_STATUS_TABLE_NAME': 'test-job-status-table',
        'EXECUTION_ERRORS_TABLE_NAME': 'test-execution-errors-table',
        'STUDY_METADATA_TABLE_NAME': 'test-study-metadata-table',
        'METRICS_TABLE_NAME': 'test-metrics-table',
        'DATASET_TABLE_NAME': 'test-dataset-table',
    })
    
    yield
    
    # Restore original environment
    os.environ.clear()
    os.environ.update(original_env)

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
            'AWS_DEFAULT_REGION': 'us-east-1'
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

@pytest.fixture
def simple_manifest_data():
    return {
        "experimentId": "test-experiment",
        "s3Path": "s3://test-bucket/test-data/",
        "inputData": {
            "format": "bed",
            "filePrefix": "sample1"
        }
    }

@pytest.fixture
def complex_manifest_data():
    return {
        "experimentId": "test-experiment",
        "s3Path": "s3://test-bucket/test-data/",
        "inputData": {
            "format": "bed",
            "filePrefix": "sample1",
            "phenoFile": "pheno.txt",
            "phenoColumns": ["phenotype1", "phenotype2"],
            "covarFile": "covar.txt",
            "covarColumns": ["age", "sex"],
            "catCovarColumns": ["sex"]
        },
        "analysisParams": {
            "traitType": "qt",
            "blockSize": 1000,
            "minMAC": 5,
            "threads": 4
        },
        "outputParams": {
            "outPrefix": "test_results",
            "outputS3Path": "s3://results-bucket/experiment1/",
            "gz": True
        },
        "userId": "user123",
        "studyId": "study456"
    }

@pytest.fixture
def s3_event():
    return {
        "Records": [
            {
                "s3": {
                    "bucket": {
                        "name": "test-bucket"
                    },
                    "object": {
                        "key": "test-data/manifest.json"
                    }
                }
            }
        ]
    }

@pytest.fixture
def sqs_event(s3_event):
    return {
        "Records": [
            {
                "body": json.dumps({
                    "Type": "Notification",
                    "Message": json.dumps(s3_event)
                })
            }
        ]
    }

@pytest.fixture
def samples_table_name():
    return "test-samples-table"

@pytest.fixture
def study_metadata_table_name():
    return "test-study-metadata-table"

@pytest.fixture
def dataset_table_name():
    return "test-dataset-table" 