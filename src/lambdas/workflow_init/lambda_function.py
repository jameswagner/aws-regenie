import json
import os
import uuid
import time
import boto3
from botocore.exceptions import ClientError
import logging
from typing import Dict, Any, Tuple, Optional

# Import shared utilities
from shared.s3_utils import ensure_trailing_slash
from shared.dynamodb_utils import get_workflow_table, get_current_timestamp
from shared.constants import (
    WorkflowStatus, JobConstants, ManifestConstants, WorkflowInitConstants, 
    ErrorMessages, GenomicFormats, EnvironmentVariables
)
from shared.logging_utils import setup_lambda_logging

# Initialize AWS clients
s3 = boto3.client('s3')

# Lambda-compatible logging setup
logger = setup_lambda_logging()


class WorkflowInitializer:
    """Handles workflow initialization logic"""
    
    def __init__(self):
        self.workflow_table = get_workflow_table()
        self.results_bucket = os.environ.get(EnvironmentVariables.RESULTS_BUCKET_NAME, '')
    
    def extract_and_validate_parameters(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Extract and validate parameters from the event (trust pre-validated input)"""
        # Extract basic parameters
        user_id = event.get('userId', WorkflowInitConstants.DEFAULT_USER_ID)
        study_id = event.get('studyId')
        
        # Extract S3 path (trust it's already validated)
        s3_path = event.get('s3Path') or event.get('datasetPath')
        if not s3_path:
            raise ValueError(ErrorMessages.MISSING_S3_PATH)
        s3_path = ensure_trailing_slash(s3_path)
        
        # Extract nested parameter structures
        input_data = event.get('inputData', {})
        analysis_params = event.get('analysisParams', {})
        output_params = event.get('outputParams', {})
        
        return {
            'user_id': user_id,
            'study_id': study_id,
            's3_path': s3_path,
            'input_data': input_data,
            'analysis_params': analysis_params,
            'output_params': output_params
        }
    
    def validate_chromosome_parameters(self, analysis_params: Dict[str, Any]) -> None:
        """Validate chromosome parameters"""
        chr_param = analysis_params.get('chr')
        chr_list = analysis_params.get('chrList')
        
        if chr_param and chr_list:
            raise ValueError(ErrorMessages.CHR_AND_CHR_LIST_CONFLICT)
    
    def determine_output_s3_path(self, output_params: Dict[str, Any], workflow_id: str, s3_path: str) -> str:
        """Determine the output S3 path using results bucket logic"""
        # If output path is already specified, use it
        if output_params.get('outputS3Path'):
            return ensure_trailing_slash(output_params['outputS3Path'])
        
        # Use dedicated results bucket if configured
        if self.results_bucket:
            return f"s3://{self.results_bucket}/workflows/{workflow_id}/"
        else:
            # Fallback to input bucket with results directory
            return f"{s3_path}results/"
    
    def ensure_output_params_with_defaults(self, output_params: Dict[str, Any], workflow_id: str, s3_path: str) -> Dict[str, Any]:
        """Ensure output parameters have all required fields with defaults"""
        # Determine output S3 path
        output_s3_path = self.determine_output_s3_path(output_params, workflow_id, s3_path)
        
        # Create output params with defaults
        return {
            'outPrefix': output_params.get('outPrefix', JobConstants.DEFAULT_OUTPUT_PREFIX),
            'outputS3Path': output_s3_path,
            'gz': output_params.get('gz', JobConstants.DEFAULT_GZ_OUTPUT)
        }
        
    def create_workflow_record(self, workflow_id: str, timestamp: str, 
                             user_id: str, study_id: Optional[str],
                             input_data: Dict[str, Any], analysis_params: Dict[str, Any], 
                             output_params: Dict[str, Any]) -> None:
        """Create workflow record in DynamoDB"""
        # Calculate TTL (30 days from now)
        ttl = int(time.time()) + WorkflowInitConstants.TTL_SECONDS
        
        workflow_record = {
            'workflowId': workflow_id,
            'createdAt': timestamp,
            'updatedAt': timestamp,
            'userId': user_id,
            'status': WorkflowStatus.INITIALIZED,
            'parameters': {
                'inputData': self._extract_input_data_params(input_data),
                'analysisParams': self._extract_analysis_params(analysis_params),
                'outputParams': self._extract_output_params(output_params)
            },
            'expiresAt': ttl
        }
        
        # Add optional fields
        if study_id:
            workflow_record['studyId'] = study_id
        
        # Add chromosome parameters if provided
        if analysis_params.get('chr'):
            workflow_record['parameters']['analysisParams']['chr'] = analysis_params['chr']
        elif analysis_params.get('chrList'):
            workflow_record['parameters']['analysisParams']['chrList'] = analysis_params['chrList']
        
        self.workflow_table.put_item(Item=workflow_record)
        logger.info(f"Workflow initialized with ID: {workflow_id}")
        
    def _extract_input_data_params(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract input data parameters with defaults"""
        return {
            'format': input_data.get('format', GenomicFormats.BED),
            'filePrefix': input_data.get('filePrefix', ''),
            'phenoFile': input_data.get('phenoFile'),
            'phenoColumns': input_data.get('phenoColumns', []),
            'covarFile': input_data.get('covarFile'),
            'covarColumns': input_data.get('covarColumns', []),
            'catCovarColumns': input_data.get('catCovarColumns', [])
        }
    
    def _extract_analysis_params(self, analysis_params: Dict[str, Any]) -> Dict[str, Any]:
        """Extract analysis parameters with defaults"""
        return {
            'traitType': analysis_params.get('traitType', JobConstants.DEFAULT_TRAIT_TYPE),
            'blockSize': analysis_params.get('blockSize', JobConstants.DEFAULT_BLOCK_SIZE),
            'minMAC': analysis_params.get('minMAC', JobConstants.DEFAULT_MIN_MAC),
            'threads': analysis_params.get('threads', JobConstants.DEFAULT_THREADS),
            'cv': analysis_params.get('cv', JobConstants.DEFAULT_CV_FOLDS),
            'lowmem': analysis_params.get('lowmem', JobConstants.DEFAULT_LOWMEM)
        }
    
    def _extract_output_params(self, output_params: Dict[str, Any]) -> Dict[str, Any]:
        """Extract output parameters with defaults"""
        return {
            'outPrefix': output_params.get('outPrefix', JobConstants.DEFAULT_OUTPUT_PREFIX),
            'outputS3Path': output_params.get('outputS3Path', ''),
            'gz': output_params.get('gz', JobConstants.DEFAULT_GZ_OUTPUT)
        }
    
    def write_params_to_s3(self, params: Dict[str, Any], output_s3_path: str, workflow_id: str) -> Optional[str]:
        """Write the input parameters as a JSON file to the output S3 prefix"""
        try:
            output_s3_path = ensure_trailing_slash(output_s3_path)
            bucket_name = output_s3_path.replace('s3://', '').split('/')[0]
            prefix = output_s3_path.replace(f's3://{bucket_name}/', '')
            
            params_json = json.dumps(params, indent=2, default=str)
            params_key = f"{prefix}workflow_params_{workflow_id}.json"
            
            s3.put_object(
                Bucket=bucket_name,
                Key=params_key,
                Body=params_json,
                ContentType=ManifestConstants.JSON_CONTENT_TYPE
            )
            
            params_s3_uri = f"s3://{bucket_name}/{params_key}"
            logger.info(f"Wrote workflow parameters to {params_s3_uri}")
            return params_s3_uri
        except Exception as e:
            logger.warning(f"Warning: Failed to write parameters to S3: {e}")
            return None
    
    def initialize_workflow(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """
        Initialize a complete workflow from the event data
        
        Args:
            event: The Lambda event containing workflow parameters
            
        Returns:
            Dictionary with workflow initialization results and metadata
        """
        # Extract and validate parameters
        params = self.extract_and_validate_parameters(event)
        
        # Validate chromosome parameters
        self.validate_chromosome_parameters(params['analysis_params'])
        
        # Generate workflow ID and timestamp
        workflow_id = event.get('workflowId', str(uuid.uuid4()))
        timestamp = get_current_timestamp()
        
        # Ensure output parameters have defaults and determine output path
        output_params_with_defaults = self.ensure_output_params_with_defaults(
            params['output_params'], workflow_id, params['s3_path']
        )
        
        # Extract parameter structures for DynamoDB record
        input_data_params = self._extract_input_data_params(params['input_data'])
        analysis_params = self._extract_analysis_params(params['analysis_params'])
        
        # Create workflow record in DynamoDB
        self.create_workflow_record(
            workflow_id, timestamp, params['user_id'], params['study_id'],
            params['input_data'], params['analysis_params'], output_params_with_defaults
        )
        
        # Get output S3 path
        output_s3_path = output_params_with_defaults['outputS3Path']
        
        # Write parameters to S3
        params_to_write = {
            'workflowId': workflow_id,
            'timestamp': timestamp,
            's3Path': params['s3_path'],
            'inputData': input_data_params,
            'analysisParams': analysis_params,
            'outputParams': output_params_with_defaults
        }
        params_s3_uri = self.write_params_to_s3(params_to_write, output_s3_path, workflow_id)
        
        # Calculate processed samples
        processed_samples = len(event.get('samples', []))
        
        # Build and return result
        return self._build_result(
            workflow_id, timestamp, params['study_id'], params['s3_path'], 
            output_s3_path, processed_samples, input_data_params, 
            analysis_params, output_params_with_defaults, params_s3_uri
        )
    
    def _build_result(self, workflow_id: str, timestamp: str, study_id: Optional[str],
                     s3_path: str, output_s3_path: str, processed_samples: int,
                     input_data_params: Dict[str, Any], analysis_params: Dict[str, Any], 
                     output_params: Dict[str, Any], params_s3_uri: Optional[str]) -> Dict[str, Any]:
        """Build the final result dictionary"""
        bucket_name = s3_path.replace('s3://', '').split('/')[0]
        prefix = s3_path.replace(f's3://{bucket_name}/', '')
        
        result = {
            'workflowId': workflow_id,
            'status': WorkflowStatus.INITIALIZED,
            'timestamp': timestamp,
            'studyId': study_id,
            's3Path': s3_path,
            'dataBucketName': bucket_name,
            'dataBucketPrefix': prefix,
            'resultsBucketName': output_s3_path.replace('s3://', '').split('/')[0],
            'resultsBucketPath': output_s3_path,
            'processedSamples': processed_samples,
            'parameters': {
                'inputData': input_data_params,
                'analysisParams': analysis_params,
                'outputParams': output_params
            }
        }
        
        if params_s3_uri:
            result['parametersFile'] = params_s3_uri
        
        return result


def handler(event: dict, context: object) -> dict:
    """
    AWS Lambda handler for workflow initialization. Initializes workflow records, and writes parameters to S3.
    Trusts that input data has been pre-validated by the manifest processor.
    Args:
        event: The event dictionary passed to the Lambda function.
        context: The Lambda context object.
    Returns:
        Dictionary with workflow initialization results and metadata.
    Raises:
        ValueError: If required parameters are missing or invalid.
    """
    try:
        logger.info(f"Received event: {json.dumps(event)}")
        
        initializer = WorkflowInitializer()
        return initializer.initialize_workflow(event)
        
    except Exception as e:
        logger.error(f"Error initializing workflow: {e}")
        raise e 