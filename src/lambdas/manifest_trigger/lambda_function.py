import json
import os
import time
import boto3
import logging

# Import manifest processing logic
from .manifest_processor import ManifestProcessor
from lambdas.shared.constants import ManifestConstants

# Initialize AWS clients
s3 = boto3.client('s3')
sfn = boto3.client('stepfunctions')

# Initialize manifest processor
manifest_processor = ManifestProcessor()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



def start_workflow(state_machine_arn, input_data, workflow_id):
    """Start the Step Function workflow"""
    try:
        # Create a consistent execution name
        timestamp = int(time.time())
        execution_name = f"{workflow_id}-{timestamp}"
        
        # Start execution
        response = sfn.start_execution(
            stateMachineArn=state_machine_arn,
            name=execution_name,
            input=json.dumps(input_data)
        )
        
        return True, response['executionArn']
    except Exception as e:
        return False, str(e)

def handle_manifest_event(record):
    """Process a single S3 event record for a manifest file"""
    try:
        # Extract bucket and key from the record
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        
        logger.info(f"Processing manifest file: s3://{bucket}/{key}")
        
        # Get the manifest file content
        response = s3.get_object(Bucket=bucket, Key=key)
        manifest_content = response['Body'].read().decode(ManifestConstants.UTF8_ENCODING)
        
        # Parse the manifest
        try:
            manifest_data = json.loads(manifest_content)
        except json.JSONDecodeError as e:
            error_msg = f"Error parsing manifest JSON: {str(e)}"
            logger.error(error_msg)
            return {
                "success": False,
                "error": error_msg,
                "manifestFile": f"s3://{bucket}/{key}"
            }
        
        # Validate the manifest against the schema
        is_valid, validation_error = manifest_processor.validate_manifest_data(manifest_data)
        if not is_valid:
            error_msg = f"Manifest validation failed: {validation_error}"
            logger.error(error_msg)
            return {
                "success": False,
                "error": error_msg,
                "manifestFile": f"s3://{bucket}/{key}"
            }
        
        # Check if all required files exist
        files_exist, files_error = manifest_processor.validate_required_files_exist(manifest_data, s3)
        if not files_exist:
            error_msg = f"File validation failed: {files_error}"
            logger.error(error_msg)
            return {
                "success": False,
                "error": error_msg,
                "manifestFile": f"s3://{bucket}/{key}"
            }
        
        # Prepare the Step Function input
        workflow_input, workflow_id = manifest_processor.prepare_workflow_input(manifest_data)
        
        # Get the state machine ARN from environment variable
        state_machine_arn = os.environ.get('STATE_MACHINE_ARN')
        if not state_machine_arn:
            error_msg = "STATE_MACHINE_ARN environment variable not set"
            logger.error(error_msg)
            return {
                "success": False,
                "error": error_msg,
                "manifestFile": f"s3://{bucket}/{key}"
            }
        
        # Start the workflow
        success, result = start_workflow(state_machine_arn, workflow_input, workflow_id)
        if success:
            logger.info(f"Started workflow execution: {result}")
            
            # Log workflow start information (previously sent as SNS notification)
            logger.info(f"GWAS Workflow Started: {workflow_id}")
            logger.info(f"Experiment ID: {manifest_data['experimentId']}")
            logger.info(f"Input data: {manifest_data['s3Path']}")
            logger.info(f"Execution ARN: {result}")
            
            return {
                "success": True,
                "workflowId": workflow_id,
                "executionArn": result,
                "manifestFile": f"s3://{bucket}/{key}"
            }
        else:
            error_msg = f"Error starting workflow: {result}"
            logger.error(error_msg)
            return {
                "success": False,
                "error": error_msg,
                "manifestFile": f"s3://{bucket}/{key}"
            }
    except Exception as e:
        error_msg = f"Error processing manifest: {str(e)}"
        logger.error(error_msg)
        return {
            "success": False,
            "error": error_msg,
            "manifestFile": f"s3://{bucket}/{key}" if 'bucket' in locals() and 'key' in locals() else "unknown"
        }

def process_sqs_message(record):
    """Process an SQS message containing an S3 event from SNS"""
    try:
        # Extract the message body
        message_body = record['body']
        
        # Parse the message
        try:
            message = json.loads(message_body)
            
            # If this is an SNS message, extract the actual message
            if 'Type' in message and message['Type'] == 'Notification':
                message = json.loads(message['Message'])
            
            # Process each record in the S3 event
            results = []
            for s3_record in message.get('Records', []):
                # Check if this is an S3 event and it's for a manifest file
                if 's3' in s3_record and manifest_processor.is_manifest_file(s3_record['s3']['object']['key']):
                    result = handle_manifest_event(s3_record)
                    results.append(result)
            
            return results
        except json.JSONDecodeError as e:
            error_msg = f"Error parsing SQS message: {str(e)}"
            logger.error(error_msg)
            return [{"success": False, "error": error_msg}]
    except Exception as e:
        error_msg = f"Error processing SQS message: {str(e)}"
        logger.error(error_msg)
        return [{"success": False, "error": error_msg}]

def handler(event: dict, context: object) -> dict:
    """
    AWS Lambda handler for manifest trigger. Processes S3/SQS events for manifest files, validates, and triggers Step Functions workflows.
    Args:
        event: The event dictionary passed to the Lambda function.
        context: The Lambda context object.
    Returns:
        Dictionary with workflow trigger results and metadata.
    """
    logger.info(f"Received event: {json.dumps(event)}")
    
    # Track overall results
    all_results = []
    
    # Process each record in the SQS event
    for record in event.get('Records', []):
        results = process_sqs_message(record)
        all_results.extend(results)
    
    # Count successes and failures
    success_count = sum(1 for result in all_results if result.get('success', False))
    failure_count = len(all_results) - success_count
    
    # Return the overall status and results
    return {
        "statusCode": 200,
        "body": json.dumps({
            "processingResults": all_results,
            "summary": {
                "totalProcessed": len(all_results),
                "successCount": success_count,
                "failureCount": failure_count
            }
        })
    } 