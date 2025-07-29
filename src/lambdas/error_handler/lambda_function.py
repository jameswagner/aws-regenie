import json
import boto3
import logging
from typing import Dict, Any, List, Optional
from shared.dynamodb_utils import (
    get_workflow_table, get_job_status_table, update_job_status, 
    get_workflow_jobs, calculate_job_stats, update_workflow_status
)
from shared.constants import (
    WorkflowStatus, JobStatus, DynamoDBConstants, 
    ErrorHandlerConstants, ErrorMessages, EnvironmentVariables
)
from shared.logging_utils import setup_lambda_logging

# Lambda-compatible logging setup
logger = setup_lambda_logging()


class ErrorEventValidator:
    """Validates error event parameters"""
    
    @staticmethod
    def validate_and_extract(event: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate and extract parameters from error event
        
        Args:
            event: Lambda error event
            
        Returns:
            Dictionary with validated parameters
            
        Raises:
            ValueError: If required parameters are missing
        """
        workflow_id = event.get(DynamoDBConstants.WORKFLOW_ID_KEY)
        failed_jobs = event.get('failedJobs', [])
        
        if not workflow_id:
            raise ValueError(ErrorMessages.MISSING_WORKFLOW_ID)
        
        if not failed_jobs:
            logger.info("No failed jobs to process")
            return {'has_failures': False}
        
        return {
            'has_failures': True,
            'workflow_id': workflow_id,
            'failed_jobs': failed_jobs
        }


class WorkflowValidator:
    """Validates workflow existence in DynamoDB"""
    
    def __init__(self):
        self.workflow_table = get_workflow_table()
    
    def validate_workflow_exists(self, workflow_id: str) -> bool:
        """
        Check if workflow exists in DynamoDB
        
        Args:
            workflow_id: Workflow identifier
            
        Returns:
            True if workflow exists, False otherwise
        """
        try:
            workflow = self.workflow_table.get_item(
                Key={DynamoDBConstants.WORKFLOW_ID_KEY: workflow_id}
            ).get('Item')
            
            if not workflow:
                logger.error(f"No workflow found for ID: {workflow_id}")
                return False
            
            return True
        except Exception as e:
            logger.error(ErrorMessages.ERROR_VALIDATING_WORKFLOW.format(workflow_id=workflow_id, error=e))
            return False


class FailedJobProcessor:
    """Processes failed jobs and updates their status"""
    
    @staticmethod
    def process_failed_jobs(workflow_id: str, failed_jobs: List[Dict[str, Any]]) -> int:
        """
        Process list of failed jobs and update their status
        
        Args:
            workflow_id: Workflow identifier
            failed_jobs: List of failed job details
            
        Returns:
            Number of jobs processed
        """
        processed_count = 0
        
        for job in failed_jobs:
            job_id = job.get(DynamoDBConstants.JOB_ID_KEY)
            error_message = job.get('errorMessage', ErrorHandlerConstants.UNKNOWN_ERROR)
            
            if not job_id:
                logger.warning(ErrorMessages.SKIPPING_JOB_WITHOUT_ID.format(job=job))
                continue
            
            try:
                update_job_status(
                    workflow_id=workflow_id,
                    job_id=job_id,
                    status=JobStatus.FAILED,
                    error_detail=error_message
                )
                processed_count += 1
                logger.info(f"Updated job {job_id} status to {JobStatus.FAILED}")
            except Exception as e:
                logger.error(ErrorMessages.FAILED_TO_UPDATE_JOB.format(job_id=job_id, error=e))
        
        return processed_count


class WorkflowStatusUpdater:
    """Updates workflow status based on job statuses"""
    
    @staticmethod
    def update_workflow_after_failures(workflow_id: str) -> Dict[str, Any]:
        """
        Update workflow status after processing failures
        
        Args:
            workflow_id: Workflow identifier
            
        Returns:
            Dictionary with job stats and workflow status
        """
        # Get all jobs for this workflow to determine overall status
        all_jobs = get_workflow_jobs(workflow_id)
        job_stats = calculate_job_stats(all_jobs)
        workflow_status = determine_workflow_status(all_jobs)
        
        # Update workflow status
        update_workflow_status(
            workflow_id=workflow_id,
            status=workflow_status,
            jobStats=job_stats
        )
        
        logger.info(f"Updated workflow {workflow_id} status to {workflow_status}")
        
        return {
            'workflow_status': workflow_status,
            'job_stats': job_stats
        }


class ErrorHandlerService:
    """Main service for handling workflow errors"""
    
    def __init__(self):
        self.validator = ErrorEventValidator()
        self.workflow_validator = WorkflowValidator()
        self.job_processor = FailedJobProcessor()
        self.status_updater = WorkflowStatusUpdater()
    
    def handle_error_event(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """
        Handle error event and process failed jobs
        
        Args:
            event: Lambda error event
            
        Returns:
            Dictionary with error handling results
        """
        # Validate event parameters
        params = self.validator.validate_and_extract(event)
        
        if not params['has_failures']:
            return {'status': ErrorHandlerConstants.NO_FAILURES}
        
        workflow_id = params['workflow_id']
        failed_jobs = params['failed_jobs']
        
        logger.info(f"Processing {len(failed_jobs)} failed job(s) for workflow: {workflow_id}")
        
        # Validate workflow exists
        if not self.workflow_validator.validate_workflow_exists(workflow_id):
            return {'status': ErrorHandlerConstants.ERROR_STATUS, 'message': ErrorHandlerConstants.WORKFLOW_NOT_FOUND}
        
        # Process failed jobs
        processed_count = self.job_processor.process_failed_jobs(workflow_id, failed_jobs)
        
        # Update workflow status
        status_info = self.status_updater.update_workflow_after_failures(workflow_id)
        
        return {
            DynamoDBConstants.WORKFLOW_ID_KEY: workflow_id,
            'status': status_info['workflow_status'],
            'processedErrors': processed_count,
            'jobStats': status_info['job_stats']
        }


def handler(event: dict, context: object) -> dict:
    """
    AWS Lambda handler for error handling. Processes failed jobs, updates job and workflow status in DynamoDB.
    Args:
        event: The event dictionary passed to the Lambda function.
        context: The Lambda context object.
    Returns:
        Dictionary with error handling results and job stats.
    """
    try:
        logger.info(f"Received error event: {json.dumps(event)}")
        
        service = ErrorHandlerService()
        return service.handle_error_event(event)
        
    except Exception as e:
        logger.error(f"Error handling failed jobs: {e}")
        raise e 