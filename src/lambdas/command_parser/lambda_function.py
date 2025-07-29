import json
import logging
from typing import Dict, Any, List, Optional

# Import shared utilities
from shared.constants import CommandConstants, ErrorMessages
from shared.logging_utils import setup_lambda_logging

# Lambda-compatible logging setup
logger = setup_lambda_logging()


class ParameterExtractor:
    """Extracts and validates parameters from the event"""
    
    @staticmethod
    def extract_basic_params(event: Dict[str, Any]) -> Dict[str, Any]:
        """Extract basic job parameters"""
        job_id = event.get(CommandConstants.JOB_ID_KEY)
        command = event.get(CommandConstants.COMMAND_KEY)
        
        if not command:
            raise ValueError(ErrorMessages.MISSING_COMMAND)
        
        return {
            'job_id': job_id,
            'command': command,
            'job_queue': event.get(CommandConstants.JOB_QUEUE_KEY),
            'job_definition': event.get(CommandConstants.JOB_DEFINITION_KEY, CommandConstants.DEFAULT_JOB_DEFINITION),
            'start_step': event.get(CommandConstants.START_STEP_KEY, '1'),
            'step_number': event.get(CommandConstants.STEP_NUMBER_KEY)
        }
    
    @staticmethod
    def extract_fsx_params(event: Dict[str, Any]) -> Dict[str, Any]:
        """Extract FSx-related parameters"""
        return {
            'use_fsx': event.get(CommandConstants.USE_FSX_KEY, False),
            'fsx_path': event.get(CommandConstants.FSX_PATH_KEY)
        }
    
    @staticmethod
    def extract_step2_params(event: Dict[str, Any]) -> Dict[str, Any]:
        """Extract Step 2 specific parameters"""
        return {
            'chromosome': event.get(CommandConstants.CHROMOSOME_NUMBER_KEY),
            'pred_list_path': event.get(CommandConstants.PRED_LIST_PATH_KEY)
        }
    
    @staticmethod
    def extract_output_params(event: Dict[str, Any]) -> Dict[str, Any]:
        """Extract output-related parameters"""
        return {
            'output_prefix': event.get(CommandConstants.OUTPUT_PREFIX_KEY)
        }


class CommandParser:
    """Handles command parsing and formatting"""
    
    @staticmethod
    def parse_command(command: str) -> List[str]:
        """
        Parse command string into format expected by AWS Batch
        
        Args:
            command: Command string to parse
            
        Returns:
            List of command components for Batch execution
        """
        if not command:
            raise ValueError(ErrorMessages.EMPTY_COMMAND)
        
        # Wrap in a shell script to ensure proper execution in the container
        return [CommandConstants.SHELL_EXECUTABLE, CommandConstants.SHELL_FLAG, command]


class JobSubmissionBuilder:
    """Builds job submission parameters for AWS Batch"""
    
    @staticmethod
    def build_job_submission(job_id: str, job_queue: str, job_definition: str, 
                           parsed_command: List[str]) -> Dict[str, Any]:
        """
        Build job submission parameters
        
        Args:
            job_id: Unique job identifier
            job_queue: AWS Batch job queue name
            job_definition: AWS Batch job definition
            parsed_command: Parsed command array
            
        Returns:
            Job submission parameters dictionary
        """
        return {
            "JobName": job_id,
            "JobQueue": job_queue,
            "JobDefinition": job_definition,
            "ContainerOverrides": {
                "Command": parsed_command
            }
        }


class AdditionalInfoBuilder:
    """Builds additional information for debugging and tracking"""
    
    @staticmethod
    def build_additional_info(step_number: int, start_step: str, fsx_params: Dict[str, Any],
                            step2_params: Dict[str, Any], output_params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Build additional information dictionary
        
        Args:
            step_number: Step number (1 or 2)
            start_step: Starting step for the workflow
            fsx_params: FSx-related parameters
            step2_params: Step 2 specific parameters  
            output_params: Output-related parameters
            
        Returns:
            Additional info dictionary
        """
        additional_info = {
            CommandConstants.STEP_NUMBER_KEY: step_number,
            CommandConstants.START_STEP_KEY: start_step,
        }
        
        # Add FSx info if available
        if fsx_params['use_fsx'] and fsx_params['fsx_path']:
            additional_info[CommandConstants.USE_FSX_KEY] = fsx_params['use_fsx']
            additional_info[CommandConstants.FSX_PATH_KEY] = fsx_params['fsx_path']
        
        # Add chromosome info for step 2 jobs
        if step_number == 2:
            if step2_params['chromosome'] is not None:
                additional_info[CommandConstants.CHROMOSOME_NUMBER_KEY] = step2_params['chromosome']
                
            if step2_params['pred_list_path'] is not None:
                additional_info[CommandConstants.PRED_LIST_PATH_KEY] = step2_params['pred_list_path']
        
        # Add output information
        if output_params['output_prefix']:
            additional_info[CommandConstants.OUTPUT_PREFIX_KEY] = output_params['output_prefix']
        
        return additional_info


class CommandParserService:
    """Main service for parsing commands and building job submissions"""
    
    def __init__(self):
        self.parameter_extractor = ParameterExtractor()
        self.command_parser = CommandParser()
        self.job_builder = JobSubmissionBuilder()
        self.info_builder = AdditionalInfoBuilder()
    
    def parse_command_event(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse command event and build job submission
        
        Args:
            event: Lambda event containing command and job parameters
            
        Returns:
            Dictionary with job submission and additional info
        """
        # Extract parameters
        basic_params = self.parameter_extractor.extract_basic_params(event)
        fsx_params = self.parameter_extractor.extract_fsx_params(event)
        step2_params = self.parameter_extractor.extract_step2_params(event)
        output_params = self.parameter_extractor.extract_output_params(event)
        
        # Parse command
        parsed_command = self.command_parser.parse_command(basic_params['command'])
        
        # Build job submission
        job_submission = self.job_builder.build_job_submission(
            basic_params['job_id'],
            basic_params['job_queue'],
            basic_params['job_definition'],
            parsed_command
        )
        
        # Build additional info
        additional_info = self.info_builder.build_additional_info(
            basic_params['step_number'],
            basic_params['start_step'],
            fsx_params,
            step2_params,
            output_params
        )
        
        return {
            "jobSubmission": job_submission,
            "additionalInfo": additional_info
        }


def handler(event: dict, context: object) -> dict:
    """
    AWS Lambda handler for parsing command strings for batch jobs. Converts command strings to the format expected by AWS Batch.
    Args:
        event: The event dictionary passed to the Lambda function.
        context: The Lambda context object.
    Returns:
        Dictionary with job submission and additional info.
    Raises:
        ValueError: If required parameters are missing or invalid.
    """
    try:
        logger.info(f"Received event: {json.dumps(event)}")
        
        service = CommandParserService()
        return service.parse_command_event(event)
        
    except Exception as e:
        logger.error(f"Error parsing command: {e}")
        raise e 