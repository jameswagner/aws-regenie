import json
import os
import time
import boto3
import traceback
import logging

# Import manifest processing logic
from .manifest_processor import ManifestProcessor
from shared.constants import ManifestConstants
from shared.logging_utils import setup_lambda_logging

# Lambda-compatible logging setup
logger = setup_lambda_logging()

# Initialize AWS clients
s3 = boto3.client('s3')
sfn = boto3.client('stepfunctions')

# Initialize manifest processor
manifest_processor = ManifestProcessor()

logger.info("=== LAMBDA MODULE LOADED ===")

def start_workflow(state_machine_arn, input_data, workflow_id):
    """Start the Step Function workflow"""
    logger.info(f"Starting workflow for {workflow_id}")
    try:
        # Create a consistent execution name
        timestamp = int(time.time())
        execution_name = f"{workflow_id}-{timestamp}"
        
        logger.info(f"Execution name: {execution_name}")
        
        # Start execution
        response = sfn.start_execution(
            stateMachineArn=state_machine_arn,
            name=execution_name,
            input=json.dumps(input_data)
        )
        
        logger.info(f"Workflow started successfully: {response['executionArn']}")
        return True, response['executionArn']
    except Exception as e:
        logger.error(f"ERROR starting workflow: {str(e)}")
        logger.error(f"TRACEBACK: {traceback.format_exc()}")
        return False, str(e)

def handle_manifest_event(s3_detail):
    """Process a single EventBridge S3 event detail for a manifest file"""
    logger.info("=== STARTING MANIFEST EVENT PROCESSING ===")
    try:
        # Extract bucket and key from EventBridge format
        bucket = s3_detail['bucket']['name']
        key = s3_detail['object']['key']
        
        logger.info(f"Processing manifest file: s3://{bucket}/{key}")
        
        # Get the manifest file content
        logger.info("Fetching manifest content from S3...")
        response = s3.get_object(Bucket=bucket, Key=key)
        manifest_content = response['Body'].read().decode(ManifestConstants.UTF8_ENCODING)
        logger.info(f"Manifest content length: {len(manifest_content)} characters")
        
        # Parse the manifest
        logger.info("Parsing manifest JSON...")
        try:
            manifest_data = json.loads(manifest_content)
            logger.info(f"Manifest data keys: {list(manifest_data.keys())}")
        except json.JSONDecodeError as e:
            error_msg = f"Error parsing manifest JSON: {str(e)}"
            logger.error(f"JSON PARSE ERROR: {error_msg}")
            return {
                "success": False,
                "error": error_msg,
                "manifestFile": f"s3://{bucket}/{key}"
            }
        
        # Validate the manifest against the schema
        logger.info("Validating manifest schema...")
        is_valid, validation_error = manifest_processor.validate_manifest_data(manifest_data)
        logger.info(f"Schema validation result: {is_valid}")
        if not is_valid:
            error_msg = f"Manifest validation failed: {validation_error}"
            logger.error(f"VALIDATION ERROR: {error_msg}")
            return {
                "success": False,
                "error": error_msg,
                "manifestFile": f"s3://{bucket}/{key}"
            }
        
        # Check if all required files exist
        logger.info("Validating required files exist...")
        files_exist, files_error = manifest_processor.validate_required_files_exist(manifest_data, s3)
        logger.info(f"File existence validation result: {files_exist}")
        if not files_exist:
            error_msg = f"File validation failed: {files_error}"
            logger.error(f"FILE VALIDATION ERROR: {error_msg}")
            return {
                "success": False,
                "error": error_msg,
                "manifestFile": f"s3://{bucket}/{key}"
            }
        
        # Prepare the Step Function input
        logger.info("Preparing workflow input...")
        workflow_input, workflow_id = manifest_processor.prepare_workflow_input(manifest_data)
        logger.info(f"Workflow ID: {workflow_id}")
        
        # Get the state machine ARN from environment variable
        state_machine_arn = os.environ.get('STATE_MACHINE_ARN')
        logger.info(f"State machine ARN: {state_machine_arn}")
        if not state_machine_arn:
            error_msg = "STATE_MACHINE_ARN environment variable not set"
            logger.error(f"ENV VAR ERROR: {error_msg}")
            return {
                "success": False,
                "error": error_msg,
                "manifestFile": f"s3://{bucket}/{key}"
            }
        
        # Start the workflow
        logger.info("Starting Step Function workflow...")
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
            logger.error(f"WORKFLOW START ERROR: {error_msg}")
            return {
                "success": False,
                "error": error_msg,
                "manifestFile": f"s3://{bucket}/{key}"
            }
    except Exception as e:
        error_msg = f"Error processing manifest: {str(e)}"
        logger.error(f"MANIFEST PROCESSING ERROR: {error_msg}")
        logger.error(f"TRACEBACK: {traceback.format_exc()}")
        return {
            "success": False,
            "error": error_msg,
            "manifestFile": f"s3://{bucket}/{key}" if 'bucket' in locals() and 'key' in locals() else "unknown"
        }

def process_sqs_message(record):
    """Process an SQS message containing an EventBridge S3 event from SNS"""
    logger.info(f"Processing SQS record: {record.get('messageId', 'unknown')}")
    
    try:
        # Extract the message body
        message_body = record['body']
        logger.info(f"Message body length: {len(message_body)}")
        
        # Parse the message
        try:
            message = json.loads(message_body)
            logger.info(f"Parsed message type: {message.get('Type', 'unknown')}")
            
            # If this is an SNS message, extract the actual message
            if 'Type' in message and message['Type'] == 'Notification':
                logger.info("Processing SNS notification message...")
                inner_message = json.loads(message['Message'])
                logger.info(f"Inner message keys: {list(inner_message.keys())}")
                message = inner_message
            
            # Process EventBridge S3 event
            results = []
            if 'detail' in message and 'bucket' in message['detail'] and 'object' in message['detail']:
                logger.info("Processing EventBridge S3 event")
                object_key = message['detail']['object']['key']
                logger.info(f"Processing S3 object: {object_key}")
                
                is_manifest = manifest_processor.is_manifest_file(object_key)
                logger.info(f"Is manifest file: {is_manifest}")
                
                if is_manifest:
                    logger.info("About to process manifest event...")
                    result = handle_manifest_event(message['detail'])
                    logger.info(f"Manifest processing result: {result}")
                    results.append(result)
                else:
                    logger.info(f"Skipping non-manifest file: {object_key}")
            else:
                logger.warning(f"Unknown event format - message keys: {list(message.keys())}")
            
            logger.info(f"Returning {len(results)} results from SQS message")
            return results
            
        except json.JSONDecodeError as e:
            error_msg = f"Error parsing SQS message: {str(e)}"
            logger.error(f"JSON DECODE ERROR: {error_msg}")
            logger.error(f"TRACEBACK: {traceback.format_exc()}")
            return [{"success": False, "error": error_msg}]
    except Exception as e:
        error_msg = f"Error processing SQS message: {str(e)}"
        logger.error(f"SQS PROCESSING ERROR: {error_msg}")
        logger.error(f"TRACEBACK: {traceback.format_exc()}")
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
    logger.info("=== LAMBDA HANDLER STARTED ===")
    logger.info(f"Received event with {len(event.get('Records', []))} records")
    
    # Track overall results
    all_results = []
    
    # Process each record in the SQS event
    for i, record in enumerate(event.get('Records', [])):
        logger.info(f"Processing record {i+1} of {len(event.get('Records', []))}")
        results = process_sqs_message(record)
        logger.info(f"Got {len(results)} results from record {i+1}")
        all_results.extend(results)
    
    logger.info(f"Total results collected: {len(all_results)}")
    
    # Count successes and failures
    success_count = sum(1 for result in all_results if result.get('success', False))
    failure_count = len(all_results) - success_count
    
    logger.info(f"Summary - Success: {success_count}, Failures: {failure_count}")
    
    # Return the overall status and results
    response = {
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
    
    logger.info("=== LAMBDA HANDLER COMPLETED ===")
    return response