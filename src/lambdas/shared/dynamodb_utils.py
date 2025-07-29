"""
DynamoDB utility functions for workflow and job status operations
"""
import os
import boto3
from datetime import datetime
from typing import Dict, List, Optional, Tuple

# Import shared constants
from .constants import WorkflowStatus, JobStatus, JobConstants, DynamoDBConstants


def get_workflow_table():
    """Get the workflow DynamoDB table"""
    dynamodb = boto3.resource('dynamodb')
    table_name = os.environ.get('WORKFLOW_TABLE_NAME', '')
    return dynamodb.Table(table_name)


def get_job_status_table():
    """Get the job status DynamoDB table"""
    dynamodb = boto3.resource('dynamodb')
    table_name = os.environ.get('JOB_STATUS_TABLE_NAME', '')
    return dynamodb.Table(table_name)


def get_current_timestamp() -> str:
    """Get current timestamp in ISO format"""
    return datetime.now(datetime.UTC).isoformat()


def update_workflow_status(workflow_id: str, status: str, **kwargs) -> None:
    """
    Update workflow status in DynamoDB
    
    Args:
        workflow_id: The workflow ID
        status: The new status
        **kwargs: Additional fields to update (e.g., jobStats, resultsBucketPath)
    """
    workflow_table = get_workflow_table()
    current_time = get_current_timestamp()
    
    # Build update expression
    update_expression = f'SET {DynamoDBConstants.STATUS_PLACEHOLDER} = {DynamoDBConstants.STATUS_VALUE_PLACEHOLDER}, {DynamoDBConstants.UPDATED_AT_ATTR} = {DynamoDBConstants.UPDATED_AT_PLACEHOLDER}'
    expression_attribute_names = {DynamoDBConstants.STATUS_PLACEHOLDER: DynamoDBConstants.STATUS_ATTR}
    expression_attribute_values = {
        DynamoDBConstants.STATUS_VALUE_PLACEHOLDER: status,
        DynamoDBConstants.UPDATED_AT_PLACEHOLDER: current_time
    }
    
    # Add additional fields
    for key, value in kwargs.items():
        placeholder = f':{key}'
        update_expression += f', {key} = {placeholder}'
        expression_attribute_values[placeholder] = value
    
    workflow_table.update_item(
        Key={JobConstants.WORKFLOW_ID_KEY: workflow_id},
        UpdateExpression=update_expression,
        ExpressionAttributeNames=expression_attribute_names,
        ExpressionAttributeValues=expression_attribute_values
    )


def update_job_status(workflow_id: str, job_id: str, status: str, error_detail: Optional[str] = None) -> None:
    """
    Update job status in DynamoDB
    
    Args:
        workflow_id: The workflow ID
        job_id: The job ID
        status: The new status
        error_detail: Error details if status is FAILED
    """
    job_status_table = get_job_status_table()
    current_time = get_current_timestamp()
    
    update_expression = f'SET {DynamoDBConstants.STATUS_PLACEHOLDER} = {DynamoDBConstants.STATUS_VALUE_PLACEHOLDER}, {DynamoDBConstants.UPDATED_AT_ATTR} = {DynamoDBConstants.UPDATED_AT_PLACEHOLDER}'
    expression_attribute_names = {DynamoDBConstants.STATUS_PLACEHOLDER: DynamoDBConstants.STATUS_ATTR}
    expression_attribute_values = {
        DynamoDBConstants.STATUS_VALUE_PLACEHOLDER: status,
        DynamoDBConstants.UPDATED_AT_PLACEHOLDER: current_time
    }
    
    if error_detail:
        update_expression += f', {DynamoDBConstants.ERROR_DETAIL_ATTR} = {DynamoDBConstants.ERROR_DETAIL_PLACEHOLDER}'
        expression_attribute_values[DynamoDBConstants.ERROR_DETAIL_PLACEHOLDER] = error_detail
    
    job_status_table.update_item(
        Key={
            JobConstants.WORKFLOW_ID_KEY: workflow_id,
            JobConstants.JOB_ID_KEY: job_id
        },
        UpdateExpression=update_expression,
        ExpressionAttributeNames=expression_attribute_names,
        ExpressionAttributeValues=expression_attribute_values
    )


def get_workflow_jobs(workflow_id: str) -> List[Dict]:
    """
    Get all jobs for a workflow
    
    Args:
        workflow_id: The workflow ID
        
    Returns:
        List of job items
    """
    job_status_table = get_job_status_table()
    response = job_status_table.query(
        KeyConditionExpression=f'{JobConstants.WORKFLOW_ID_KEY} = {DynamoDBConstants.WORKFLOW_ID_PLACEHOLDER}',
        ExpressionAttributeValues={DynamoDBConstants.WORKFLOW_ID_PLACEHOLDER: workflow_id}
    )
    return response.get('Items', [])


def calculate_job_stats(jobs: List[Dict]) -> Dict:
    """
    Calculate job statistics from a list of jobs
    
    Args:
        jobs: List of job items
        
    Returns:
        Dictionary with job statistics
    """
    total_jobs = len(jobs)
    completed_jobs = sum(1 for job in jobs if job.get(DynamoDBConstants.STATUS_ATTR) == JobStatus.COMPLETED)
    failed_jobs = sum(1 for job in jobs if job.get(DynamoDBConstants.STATUS_ATTR) == JobStatus.FAILED)
    pending_jobs = sum(1 for job in jobs if job.get(DynamoDBConstants.STATUS_ATTR) in [JobStatus.PENDING, JobStatus.RUNNING])
    
    return {
        DynamoDBConstants.TOTAL_KEY: total_jobs,
        DynamoDBConstants.COMPLETED_KEY: completed_jobs,
        DynamoDBConstants.FAILED_KEY: failed_jobs,
        DynamoDBConstants.PENDING_KEY: pending_jobs
    }


def determine_workflow_status(jobs: List[Dict]) -> str:
    """
    Determine workflow status based on job statuses
    
    Args:
        jobs: List of job items
        
    Returns:
        Workflow status string
    """
    if not jobs:
        return WorkflowStatus.IN_PROGRESS
    
    job_stats = calculate_job_stats(jobs)
    
    # Check if any Step 1 jobs failed (critical failure)
    step1_failed = any(
        job.get(DynamoDBConstants.STEP_NUMBER_ATTR) == JobConstants.STEP_1 and 
        job.get(DynamoDBConstants.STATUS_ATTR) == JobStatus.FAILED 
        for job in jobs
    )
    
    if step1_failed:
        return WorkflowStatus.FAILED
    elif job_stats[DynamoDBConstants.PENDING_KEY] == 0:
        return WorkflowStatus.COMPLETED_WITH_ERRORS if job_stats[DynamoDBConstants.FAILED_KEY] > 0 else WorkflowStatus.COMPLETED
    else:
        return WorkflowStatus.IN_PROGRESS 