import json
import os
import tempfile
import logging
from typing import Dict, List, Any, Optional

# Import shared utilities
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from lambdas.shared.s3_utils import ensure_trailing_slash
from lambdas.shared.dynamodb_utils import (
    get_workflow_table, get_job_status_table, get_current_timestamp, update_workflow_status
)
from lambdas.shared.constants import (
    WorkflowStatus, JobStatus, JobConstants, GenomicFormats, FormatFileMapping, ErrorMessages
)

# Initialize AWS clients
import boto3
s3 = boto3.client('s3')

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ChromosomeDetector:
    """Handles chromosome detection from genomic files"""
    
    @staticmethod
    def get_chromosomes_from_bim(s3_path: str, file_prefix: str, data_format: str) -> List[str]:
        """
        Parse a BIM or PVAR file from S3 to extract the unique chromosome names.
        
        Args:
            s3_path: S3 path to the dataset (e.g. s3://bucket-name/data/path/)
            file_prefix: Prefix for the genetic data files
            data_format: Format of the genetic data (bed, pgen, bgen)
            
        Returns:
            List of unique chromosome names found in the BIM/PVAR file
        """
        try:
            # Parse S3 path
            parts = s3_path.replace('s3://', '').split('/')
            bucket = parts[0]
            prefix = '/'.join(parts[1:])
            
            # Determine file extension based on data format
            if data_format == GenomicFormats.BED:
                file_extension = FormatFileMapping.CHROMOSOME_DETECTION_EXTENSIONS[GenomicFormats.BED]
            elif data_format == GenomicFormats.PGEN:
                file_extension = FormatFileMapping.CHROMOSOME_DETECTION_EXTENSIONS[GenomicFormats.PGEN]
            else:  # bgen format doesn't have chromosome info in a separate file
                logger.info(ErrorMessages.UNSUPPORTED_FORMAT_FOR_CHROMOSOME_DETECTION.format(format=data_format))
                return JobConstants.DEFAULT_CHROMOSOMES
            
            # Look for BIM/PVAR file
            variant_file_key = f"{prefix}{file_prefix}{file_extension}"
            
            return ChromosomeDetector._process_variant_file(bucket, variant_file_key, data_format)
            
        except Exception as e:
            logger.error(f"Error reading variant file, using default chromosomes: {e}")
            return JobConstants.DEFAULT_CHROMOSOMES
    
    @staticmethod
    def _process_variant_file(bucket: str, variant_file_key: str, data_format: str) -> List[str]:
        """Process the variant file to extract chromosomes"""
        temp_file_path = None
        try:
            # Create and close the temp file first
            with tempfile.NamedTemporaryFile(mode='wb', delete=False) as temp_file:
                temp_file_path = temp_file.name
            
            # Download and process file
            s3.download_file(bucket, variant_file_key, temp_file_path)
            
            chromosomes = set()
            with open(temp_file_path, 'r') as variant_file:
                for line in variant_file:
                    if line.startswith('#'):  # Skip header lines in pvar files
                        continue
                    fields = line.strip().split()
                    
                    if data_format == GenomicFormats.BED and len(fields) < 6:
                        raise ValueError(ErrorMessages.MALFORMED_BIM)
                    elif data_format == GenomicFormats.PGEN and len(fields) < 1:
                        raise ValueError(ErrorMessages.MALFORMED_PVAR)
                    
                    chromosomes.add(fields[0])
            
            return sorted(list(chromosomes))
            
        finally:
            # Clean up temp file
            if temp_file_path and os.path.exists(temp_file_path):
                try:
                    os.unlink(temp_file_path)
                except Exception as e:
                    logger.warning(f"Warning: Could not delete temp file: {e}")
                

class CommandGenerator:
    """Generates executable regenie commands for GWAS jobs"""
    
    @staticmethod
    def generate_step1_command(parameters: Dict[str, Any]) -> str:
        """Generate Step 1 regenie command for model building"""
        cmd_parts = [
            "regenie",
            "--step", "1",
            "--bed", f"{parameters['fsxDataPath']}/{parameters['filePrefix']}",
            "--phenoFile", f"{parameters['fsxDataPath']}/{parameters['phenoFile']}" if parameters.get('phenoFile') else None,
            "--phenoCol", ",".join(parameters.get('phenoColumns', [])) if parameters.get('phenoColumns') else None,
            "--bsize", str(parameters.get('blockSize', JobConstants.DEFAULT_BLOCK_SIZE)),
            "--cv", str(parameters.get('cvFolds', JobConstants.DEFAULT_CV_FOLDS)),
            "--loocv" if parameters.get('traitType') == 'bt' else "--qt",
            "--lowmem" if parameters.get('lowmem', JobConstants.DEFAULT_LOWMEM) else None,
            "--threads", str(parameters.get('threads', JobConstants.DEFAULT_THREADS)),
            "--out", f"{parameters['fsxOutputPath']}/{parameters.get('outPrefix', JobConstants.DEFAULT_OUTPUT_PREFIX)}"
        ]
        
        # Add covariate file if provided
        if parameters.get('covarFile'):
            cmd_parts.extend([
                "--covarFile", f"{parameters['fsxDataPath']}/{parameters['covarFile']}",
                "--covarCol", ",".join(parameters.get('covarColumns', [])) if parameters.get('covarColumns') else None,
                "--catCovarList", ",".join(parameters.get('catCovarColumns', [])) if parameters.get('catCovarColumns') else None
            ])
        
        # Add gzip option if enabled
        if parameters.get('gzOutput', JobConstants.DEFAULT_GZ_OUTPUT):
            cmd_parts.append("--gz")
            
        # Filter out None values and join
        return " ".join(filter(None, cmd_parts))
    
    @staticmethod
    def generate_step2_command(parameters: Dict[str, Any]) -> str:
        """Generate Step 2 regenie command for association testing"""
        cmd_parts = [
            "regenie",
            "--step", "2",
            "--bed", f"{parameters['fsxDataPath']}/{parameters['filePrefix']}",
            "--phenoFile", f"{parameters['fsxDataPath']}/{parameters['phenoFile']}" if parameters.get('phenoFile') else None,
            "--phenoCol", ",".join(parameters.get('phenoColumns', [])) if parameters.get('phenoColumns') else None,
            "--pred", parameters.get('predictionFile'),
            "--chr", str(parameters.get('chromosome')),
            "--bsize", str(parameters.get('blockSize', JobConstants.DEFAULT_BLOCK_SIZE)),
            "--minMAC", str(parameters.get('minMAC', JobConstants.DEFAULT_MIN_MAC)),
            "--loocv" if parameters.get('traitType') == 'bt' else "--qt", 
            "--threads", str(parameters.get('threads', JobConstants.DEFAULT_THREADS)),
            "--out", f"{parameters['fsxOutputPath']}/{parameters.get('outPrefix', JobConstants.DEFAULT_OUTPUT_PREFIX)}"
        ]
        
        # Add covariate file if provided
        if parameters.get('covarFile'):
            cmd_parts.extend([
                "--covarFile", f"{parameters['fsxDataPath']}/{parameters['covarFile']}",
                "--covarCol", ",".join(parameters.get('covarColumns', [])) if parameters.get('covarColumns') else None,
                "--catCovarList", ",".join(parameters.get('catCovarColumns', [])) if parameters.get('catCovarColumns') else None
            ])
        
        # Add gzip option if enabled
        if parameters.get('gzOutput', JobConstants.DEFAULT_GZ_OUTPUT):
            cmd_parts.append("--gz")
            
        # Filter out None values and join
        return " ".join(filter(None, cmd_parts))


class PathMapper:
    """Handles S3 to FSx path mapping"""
    
    def __init__(self):
        self.fsx_input_mount = os.environ.get('FSX_INPUT_MOUNT_PATH', '/mnt/fsx/input')
        self.fsx_output_mount = os.environ.get('FSX_OUTPUT_MOUNT_PATH', '/mnt/fsx/output')
        self.data_prefix = os.environ.get('DATA_PREFIX', 'genomics')
    
    def map_s3_data_to_fsx_input(self, s3_path: str, analysis_subdir: str) -> str:
        """Map data bucket S3 path to FSx input mount"""
        bucket_name = s3_path.replace('s3://', '').split('/')[0]
        prefix = s3_path.replace(f's3://{bucket_name}/', '')
        
        fsx_path = f"{self.fsx_input_mount}/{self.data_prefix}/{analysis_subdir}/"
        
        # If prefix starts with data_prefix, remove it since it's already in the FSx mount
        if prefix.startswith(f"{self.data_prefix}/"):
            fsx_data_path = f"{fsx_path}/{prefix[len(self.data_prefix)+1:]}"
        else:
            fsx_data_path = f"{fsx_path}/{prefix}"
        
        logger.info(f"Mapped data S3 path {s3_path} to FSx input path {fsx_data_path}")
        return fsx_data_path
    
    def map_s3_results_to_fsx_output(self, output_s3_path: str) -> str:
        """Map results bucket S3 path to FSx output mount"""
        bucket_name = output_s3_path.replace('s3://', '').split('/')[0]
        prefix = output_s3_path.replace(f's3://{bucket_name}/', '')
        
        # For results, we create a direct mapping to the output mount
        fsx_output_path = f"{self.fsx_output_mount}/{prefix}"
        
        logger.info(f"Mapped results S3 path {output_s3_path} to FSx output path {fsx_output_path}")
        return fsx_output_path
    
    # Keep the original method for backward compatibility, but use input mount
    def map_s3_to_fsx(self, s3_path: str, analysis_subdir: str) -> str:
        """Map S3 path to FSx path (backward compatibility - uses input mount)"""
        return self.map_s3_data_to_fsx_input(s3_path, analysis_subdir)


class JobFactory:
    """Creates batch jobs for GWAS steps"""
    
    def __init__(self, job_status_table):
        self.job_status_table = job_status_table
    
    def create_step1_job(self, workflow_id: str, timestamp: str, input_data: Dict[str, Any],
                        analysis_params: Dict[str, Any], output_params: Dict[str, Any],
                        s3_path: str, output_s3_path: str, fsx_data_path: str, fsx_output_path: str) -> Dict[str, Any]:
        """Create Step 1 job"""
        job_parameters = {
            'dataFormat': input_data.get('format', ''),
            'filePrefix': input_data.get('filePrefix', ''),
            'phenoFile': input_data.get('phenoFile', ''),
            'phenoColumns': input_data.get('phenoColumns', []),
            'traitType': analysis_params.get('traitType', JobConstants.DEFAULT_TRAIT_TYPE),
            'blockSize': analysis_params.get('blockSize', JobConstants.DEFAULT_BLOCK_SIZE),
            'minMAC': analysis_params.get('minMAC', JobConstants.DEFAULT_MIN_MAC),
            'threads': analysis_params.get('threads', JobConstants.DEFAULT_THREADS),
            'cvFolds': analysis_params.get('cv', JobConstants.DEFAULT_CV_FOLDS),
            'lowmem': analysis_params.get('lowmem', JobConstants.DEFAULT_LOWMEM),
            'outPrefix': output_params.get('outPrefix', JobConstants.DEFAULT_OUTPUT_PREFIX),
            's3InputPath': s3_path,
            's3OutputPath': output_s3_path,
            'fsxDataPath': fsx_data_path,
            'fsxOutputPath': fsx_output_path,
            'gzOutput': output_params.get('gz', JobConstants.DEFAULT_GZ_OUTPUT)
        }
        
        # Add covariate parameters if provided
        if input_data.get('covarFile'):
            job_parameters.update({
                'covarFile': input_data.get('covarFile'),
                'covarColumns': input_data.get('covarColumns', []),
                'catCovarColumns': input_data.get('catCovarColumns', [])
            })
        
        # Generate executable command
        command = CommandGenerator.generate_step1_command(job_parameters)
        
        step1_job = {
            JobConstants.JOB_ID_KEY: JobConstants.STEP1_JOB_PATTERN.format(workflow_id=workflow_id),
            'stepNumber': JobConstants.STEP_1,
            'command': command,  # ADD EXECUTABLE COMMAND
            JobConstants.WORKFLOW_ID_KEY: workflow_id,
            'status': JobStatus.PENDING,
            'createdAt': timestamp,
            'updatedAt': timestamp,
            'parameters': job_parameters
        }
        
        # Save job to DynamoDB
        self.job_status_table.put_item(Item=step1_job)
        return step1_job
    
    def create_step2_jobs(self, workflow_id: str, timestamp: str, chromosomes: List[str],
                         input_data: Dict[str, Any], analysis_params: Dict[str, Any], 
                         output_params: Dict[str, Any], s3_path: str, output_s3_path: str,
                         fsx_data_path: str, fsx_output_path: str, prediction_file: str) -> List[Dict[str, Any]]:
        """Create Step 2 jobs for each chromosome"""
        jobs = []
        out_prefix = output_params.get('outPrefix', JobConstants.DEFAULT_OUTPUT_PREFIX)
        
        for chrom in chromosomes:
            # Build job parameters for this chromosome
            job_parameters = {
                'dataFormat': input_data.get('format', ''),
                'filePrefix': input_data.get('filePrefix', ''),
                'phenoFile': input_data.get('phenoFile', ''),
                'phenoColumns': input_data.get('phenoColumns', []),
                'traitType': analysis_params.get('traitType', JobConstants.DEFAULT_TRAIT_TYPE),
                'blockSize': analysis_params.get('blockSize', JobConstants.DEFAULT_BLOCK_SIZE),
                'minMAC': analysis_params.get('minMAC', JobConstants.DEFAULT_MIN_MAC),
                'threads': analysis_params.get('threads', JobConstants.DEFAULT_THREADS),
                'chromosome': str(chrom),
                'predictionFile': prediction_file,
                'outPrefix': f"{out_prefix}_chr{chrom}",
                's3InputPath': s3_path,
                's3OutputPath': output_s3_path,
                'fsxDataPath': fsx_data_path,
                'fsxOutputPath': fsx_output_path,
                'gzOutput': output_params.get('gz', JobConstants.DEFAULT_GZ_OUTPUT)
            }
            
            # Add covariate parameters if provided
            if input_data.get('covarFile'):
                job_parameters.update({
                    'covarFile': input_data.get('covarFile'),
                    'covarColumns': input_data.get('covarColumns', []),
                    'catCovarColumns': input_data.get('catCovarColumns', [])
                })
            
            # Generate executable command
            command = CommandGenerator.generate_step2_command(job_parameters)
            
            step2_job = {
                JobConstants.JOB_ID_KEY: JobConstants.STEP2_JOB_PATTERN.format(workflow_id=workflow_id, chrom=chrom),
                'stepNumber': JobConstants.STEP_2,
                'command': command,  # ADD EXECUTABLE COMMAND
                JobConstants.WORKFLOW_ID_KEY: workflow_id,
                'status': JobStatus.PENDING,
                'createdAt': timestamp,
                'updatedAt': timestamp,
                'parameters': job_parameters
            }
            
            # Save job to DynamoDB
            self.job_status_table.put_item(Item=step2_job)
            jobs.append(step2_job)
        
        return jobs
    

class JobCalculator:
    """Main job calculation orchestrator"""
    
    def __init__(self):
        self.workflow_table = get_workflow_table()
        self.job_status_table = get_job_status_table()
        self.path_mapper = PathMapper()
        self.job_factory = JobFactory(self.job_status_table)
    
    def calculate_jobs(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate and create jobs for a GWAS workflow
        
        Args:
            event: Lambda event containing workflow parameters
            
        Returns:
            Dictionary with job calculation results
        """
        # Extract and validate basic parameters
        workflow_id = self._extract_workflow_id(event)
        timestamp = get_current_timestamp()
        
        # Update workflow status to calculating
        update_workflow_status(workflow_id, WorkflowStatus.CALCULATING_JOBS)
        
        # Extract parameter groups
        params = self._extract_parameters(event)
        
        # Determine chromosomes to process
        chromosomes = self._determine_chromosomes(params)
        
        # Calculate FSx paths
        fsx_data_path = self.path_mapper.map_s3_data_to_fsx_input(
            params['s3_path'], params['analysis_subdir']
        )
        fsx_output_path = self.path_mapper.map_s3_results_to_fsx_output(
            params['output_s3_path']
        )
        
        # Create jobs based on start step
        jobs = []
        prediction_file = None
        start_step = event.get('startStep', JobConstants.STEP_1)
        
        if start_step == JobConstants.STEP_2:
            prediction_file = event.get('predictionFile', '')
            if not prediction_file:
                raise ValueError(ErrorMessages.MISSING_PREDICTION_FILE)
        
        # Create Step 1 job if needed
        if start_step == JobConstants.STEP_1:
            step1_job = self.job_factory.create_step1_job(
                workflow_id, timestamp, params['input_data'], params['analysis_params'],
                params['output_params'], params['s3_path'], params['output_s3_path'], 
                fsx_data_path, fsx_output_path
            )
            jobs.append(step1_job)
            
            # Prediction file will be created by Step 1 (use output path)
            out_prefix = params['output_params'].get('outPrefix', JobConstants.DEFAULT_OUTPUT_PREFIX)
            prediction_file = f"{fsx_output_path}/{JobConstants.PREDICTION_FILE_PATTERN.format(out_prefix=out_prefix)}"
        
        # Create Step 2 jobs
        step2_jobs = self.job_factory.create_step2_jobs(
            workflow_id, timestamp, chromosomes, params['input_data'], params['analysis_params'],
            params['output_params'], params['s3_path'], params['output_s3_path'], 
            fsx_data_path, fsx_output_path, prediction_file
        )
        jobs.extend(step2_jobs)
        
        # Update workflow with job details
        self._update_workflow_with_jobs(workflow_id, jobs, chromosomes, start_step, prediction_file)
        
        # Return results
        return {
            JobConstants.WORKFLOW_ID_KEY: workflow_id,
            'startStep': start_step,
            'jobCount': len(jobs),
            'step1Jobs': [job for job in jobs if job['stepNumber'] == JobConstants.STEP_1],
            'step2Jobs': [job for job in jobs if job['stepNumber'] == JobConstants.STEP_2],
            'chromosomes': chromosomes,
            'predictionFile': prediction_file
        }
    
    def _extract_workflow_id(self, event: Dict[str, Any]) -> str:
        """Extract and validate workflow ID"""
        workflow_id = event.get(JobConstants.WORKFLOW_ID_KEY)
        if not workflow_id:
            raise ValueError(ErrorMessages.MISSING_WORKFLOW_ID)
        return workflow_id
    
    def _extract_parameters(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Extract and organize parameters from event"""
        analysis_subdir = event.get('analysisSubdir')
        if not analysis_subdir:
            raise ValueError(ErrorMessages.MISSING_ANALYSIS_SUBDIR)
        
        s3_path = event.get('s3Path', '')
        output_s3_path = event.get('outputParams', {}).get('outputS3Path', '')
        
        return {
            's3_path': s3_path,
            'output_s3_path': output_s3_path,
            'analysis_subdir': analysis_subdir,
            'input_data': event.get('inputData', {}),
            'analysis_params': event.get('analysisParams', {}),
            'output_params': event.get('outputParams', {})
        }
    
    def _determine_chromosomes(self, params: Dict[str, Any]) -> List[str]:
        """Determine which chromosomes to process"""
        analysis_params = params['analysis_params']
        chr_param = analysis_params.get('chr', '')
        chr_list = analysis_params.get('chrList', [])
        
        if chr_param:
            return [chr_param]
        elif chr_list:
            return chr_list
        else:
            # Auto-detect chromosomes
            data_format = params['input_data'].get('format', '')
            if data_format == GenomicFormats.BED:
                file_prefix = params['input_data'].get('filePrefix', '')
                return ChromosomeDetector.get_chromosomes_from_bim(
                    params['s3_path'], file_prefix, data_format
                )
            else:
                return [str(i) for i in range(1, 23)] + ['X']
    
    def _update_workflow_with_jobs(self, workflow_id: str, jobs: List[Dict[str, Any]], 
                                  chromosomes: List[str], start_step: int, prediction_file: str) -> None:
        """Update workflow record with job details"""
        update_workflow_status(
            workflow_id, 
            WorkflowStatus.JOBS_CALCULATED,
            jobCount=len(jobs),
            chromosomes=chromosomes,
            startStep=start_step,
            predictionFile=prediction_file
        )


def handler(event: dict, context: object) -> dict:
    """
    AWS Lambda handler for job calculation. Processes the event to calculate jobs for a workflow, updates DynamoDB, and returns job details.
    Args:
        event: The event dictionary passed to the Lambda function.
        context: The Lambda context object.
    Returns:
        Dictionary with job calculation results and metadata.
    Raises:
        ValueError: If required parameters are missing or invalid.
    """
    try:
        logger.info(f"Received job calculation event: {json.dumps(event)}")
        
        calculator = JobCalculator()
        return calculator.calculate_jobs(event)
        
    except Exception as e:
        logger.error(f"Error calculating jobs: {e}")
        raise e 