import json
import boto3
import logging
from typing import Dict, Any, List, Optional

# Import shared utilities
from shared.dynamodb_utils import update_workflow_status, get_workflow_jobs, calculate_job_stats
from shared.constants import (
    WorkflowStatus, JobStatus, DynamoDBConstants, 
    ErrorHandlerConstants, EnvironmentVariables, JobConstants, WorkflowInitConstants, ErrorMessages
)
from shared.logging_utils import setup_lambda_logging

# Lambda-compatible logging setup
logger = setup_lambda_logging()


class SuccessEventValidator:
    """Validates success event parameters"""
    
    @staticmethod
    def validate_and_extract(event: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate and extract parameters from success event
        
        Args:
            event: Lambda success event
            
        Returns:
            Dictionary with validated parameters
            
        Raises:
            ValueError: If required parameters are missing
        """
        workflow_id = event.get(JobConstants.WORKFLOW_ID_KEY)
        if not workflow_id:
            raise ValueError(ErrorMessages.MISSING_WORKFLOW_ID)
        
        return {
            'workflow_id': workflow_id,
            'results_bucket_path': event.get('resultsBucketPath'),
            'completion_time': event.get('completionTime')
        }


class JobStatsCalculator:
    """Calculates job statistics for workflow completion"""
    
    @staticmethod
    def calculate_final_stats(workflow_id: str) -> Dict[str, Any]:
        """
        Calculate final job statistics for the completed workflow
        
        Args:
            workflow_id: Workflow identifier
            
        Returns:
            Dictionary with job statistics
        """
        try:
            all_jobs = get_workflow_jobs(workflow_id)
            job_stats = calculate_job_stats(all_jobs)
            
            logger.info(f"Final job stats for workflow {workflow_id}: {job_stats}")
            return job_stats
            
        except Exception as e:
            logger.error(ErrorMessages.ERROR_CALCULATING_JOB_STATS.format(workflow_id=workflow_id, error=e))
            return WorkflowInitConstants.DEFAULT_JOB_STATS


class WorkflowCompletionUpdater:
    """Updates workflow status for successful completion"""
    
    @staticmethod
    def mark_workflow_completed(workflow_id: str, job_stats: Dict[str, Any],
                              results_bucket_path: Optional[str] = None,
                              completion_time: Optional[str] = None) -> None:
        """
        Mark workflow as completed and update related fields
        
        Args:
            workflow_id: Workflow identifier
            job_stats: Job statistics dictionary
            results_bucket_path: Optional path to results bucket
            completion_time: Optional completion timestamp
        """
        # Prepare additional fields to update
        update_fields = {'jobStats': job_stats}
        
        if results_bucket_path:
            update_fields['resultsBucketPath'] = results_bucket_path
            
        if completion_time:
            update_fields['completionTime'] = completion_time
        
        # Update workflow status to COMPLETED
        update_workflow_status(
            workflow_id=workflow_id,
            status=WorkflowStatus.COMPLETED,
            **update_fields
        )
        
        logger.info(f"Successfully marked workflow {workflow_id} as {WorkflowStatus.COMPLETED}")


class SuccessResponseBuilder:
    """Builds success response for the handler"""
    
    @staticmethod
    def build_success_response(workflow_id: str, job_stats: Dict[str, Any],
                             results_bucket_path: Optional[str] = None) -> Dict[str, Any]:
        """
        Build success response dictionary
        
        Args:
            workflow_id: Workflow identifier
            job_stats: Job statistics
            results_bucket_path: Optional results bucket path
            
        Returns:
            Success response dictionary
        """
        response = {
            JobConstants.WORKFLOW_ID_KEY: workflow_id,
            'status': WorkflowStatus.COMPLETED,
            'jobStats': job_stats,
            'message': ErrorHandlerConstants.WORKFLOW_COMPLETED_SUCCESSFULLY
        }
        
        if results_bucket_path:
            response['resultsBucketPath'] = results_bucket_path
        
        return response


class SuccessHandlerService:
    """Main service for handling successful workflow completion"""
    
    def __init__(self):
        self.validator = SuccessEventValidator()
        self.stats_calculator = JobStatsCalculator()
        self.completion_updater = WorkflowCompletionUpdater()
        self.response_builder = SuccessResponseBuilder()
    
    def handle_success_event(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """
        Handle successful workflow completion event
        
        Args:
            event: Lambda success event
            
        Returns:
            Dictionary with success completion results
        """
        # Validate and extract parameters
        params = self.validator.validate_and_extract(event)
        workflow_id = params['workflow_id']
        
        logger.info(f"Processing successful completion for workflow: {workflow_id}")
        
        # Calculate final job statistics
        job_stats = self.stats_calculator.calculate_final_stats(workflow_id)
        
        # Update workflow as completed
        self.completion_updater.mark_workflow_completed(
            workflow_id=workflow_id,
            job_stats=job_stats,
            results_bucket_path=params['results_bucket_path'],
            completion_time=params['completion_time']
        )
        
        # Build and return success response
        return self.response_builder.build_success_response(
            workflow_id=workflow_id,
            job_stats=job_stats,
            results_bucket_path=params['results_bucket_path']
        )


def handler(event: dict, context: object) -> dict:
    """
    AWS Lambda handler for successful workflow completion. Updates workflow status to COMPLETED in DynamoDB.
    
    Args:
        event: The event dictionary passed to the Lambda function
        context: The Lambda context object
        
    Returns:
        Dictionary with success completion results
    """
    try:
        logger.info(f"Received success event: {json.dumps(event)}")
        
        service = SuccessHandlerService()
        return service.handle_success_event(event)
        
    except Exception as e:
        logger.error(f"Error handling successful completion: {e}")
        raise e 