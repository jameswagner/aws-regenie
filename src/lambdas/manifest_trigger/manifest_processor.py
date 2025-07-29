"""
Manifest processing logic for GWAS workflows.
Handles validation, file checking, and transformation of manifest files.
"""
import os
import uuid
from datetime import datetime
import jsonschema
from jsonschema import validate
import logging

# Import shared utilities
from lambdas.shared.s3_utils import ensure_trailing_slash, parse_s3_uri, check_file_exists
from lambdas.shared.constants import (
    GenomicFormats, ManifestConstants, 
    FormatFileMapping, ErrorMessages
)

logger = logging.getLogger(__name__)


class ManifestProcessor:
    """Handles manifest file validation and processing for GWAS workflows"""
    
    def __init__(self):
        self.schema = self._build_manifest_schema()
    
    def _build_manifest_schema(self):
        """Build the manifest validation schema dynamically"""
        data_prefix = os.environ.get('DATA_PREFIX', ManifestConstants.DEFAULT_DATA_PREFIX)
        return {
            "type": "object",
            "required": ["experimentId", "s3Path", "inputData"],
            "properties": {
                "experimentId": {"type": "string"},
                "s3Path": {
                    "type": "string",
                    "pattern": f"^s3://[^/]+/{data_prefix}/.+$"
                },
                "inputData": {
                    "type": "object",
                    "required": ["format", "filePrefix"],
                    "properties": {
                        "format": {
                            "type": "string",
                            "enum": GenomicFormats.ALL_FORMATS
                        },
                        "filePrefix": {"type": "string"},
                        "phenoFile": {"type": "string"},
                        "phenoColumns": {
                            "type": "array",
                            "items": {"type": "string"}
                        },
                        "covarFile": {"type": "string"},
                        "covarColumns": {
                            "type": "array",
                            "items": {"type": "string"}
                        },
                        "catCovarColumns": {
                            "type": "array",
                            "items": {"type": "string"}
                        }
                    }
                }
            }
        }
    
    def is_manifest_file(self, key: str) -> bool:
        """
        Check if the file is a manifest file by its name or extension.
        
        Args:
            key: The S3 object key/filename
            
        Returns:
            bool: True if the file matches manifest file patterns, False otherwise
        """
        lowercase_key = key.lower()
        
        # Check specific manifest file patterns
        if lowercase_key.endswith(ManifestConstants.MANIFEST_FILE_SUFFIX):
            return True
        
        filename = os.path.basename(lowercase_key)
        if filename == ManifestConstants.MANIFEST_FILENAME:
            return True

        return False
    
    def validate_manifest_data(self, manifest_data: dict) -> tuple[bool, str | None]:
        """Validate the manifest data against the schema"""
        try:
            validate(instance=manifest_data, schema=self.schema)
            return True, None
        except jsonschema.exceptions.ValidationError as e:
            return False, str(e)
    
    def validate_required_files_exist(self, manifest_data: dict, s3_client) -> tuple[bool, str | None]:
        """Check if all files required by the manifest exist in S3"""
        s3_path = ensure_trailing_slash(manifest_data['s3Path'])
        bucket, prefix = parse_s3_uri(s3_path)
        
        if not bucket or not prefix:
            return False, ErrorMessages.INVALID_S3_PATH.format(path=s3_path)
        
        input_data = manifest_data['inputData']
        file_prefix = input_data['filePrefix']
        data_format = input_data['format']
        
        # Define required files based on format using constants
        required_files = []
        format_extensions = FormatFileMapping.FORMAT_EXTENSIONS.get(data_format, [])
        
        for extension in format_extensions:
            required_files.append(f"{prefix}{file_prefix}{extension}")
        
        # Check for pheno and covar files if specified
        if 'phenoFile' in input_data and input_data['phenoFile']:
            required_files.append(f"{prefix}{input_data['phenoFile']}")
        
        if 'covarFile' in input_data and input_data['covarFile']:
            required_files.append(f"{prefix}{input_data['covarFile']}")
        
        # Check each required file
        missing_files = []
        for file_key in required_files:
            if not check_file_exists(s3_client, bucket, file_key):
                missing_files.append(file_key)
        
        if missing_files:
            return False, ErrorMessages.MISSING_FILES.format(files=', '.join(missing_files))
        
        return True, None
    
    def prepare_workflow_input(self, manifest_data: dict) -> tuple[dict, str]:
        """Prepare the input for the Step Function workflow"""
        # Generate a unique workflow ID if not provided
        workflow_id = manifest_data.get('workflowId', 
                                       f"gwas-{manifest_data['experimentId']}-{str(uuid.uuid4())[:8]}")
        
        # Extract the analysis subdir from the S3 path
        s3_path = ensure_trailing_slash(manifest_data['s3Path'])
        bucket, prefix = parse_s3_uri(s3_path)
        data_prefix = os.environ.get('DATA_PREFIX', ManifestConstants.DEFAULT_DATA_PREFIX)
        analysis_subdir = prefix.replace(f"{data_prefix}/", "", 1)
        
        # Create the basic structure with the required fields
        step_function_input = {
            "workflowId": workflow_id,
            "s3Path": s3_path,
            "analysisSubdir": analysis_subdir,
            "inputData": manifest_data['inputData'],
            "timestamp": datetime.utcnow().isoformat()
        }
        
        # Add optional analysis parameters if present
        if 'analysisParams' in manifest_data:
            step_function_input['analysisParams'] = manifest_data['analysisParams']
        
        # Add output parameters if present (pass through without modification)
        if 'outputParams' in manifest_data:
            step_function_input['outputParams'] = manifest_data['outputParams']
        
        # Add user ID if present
        if 'userId' in manifest_data:
            step_function_input['userId'] = manifest_data['userId']
        
        # Add study ID if present
        if 'studyId' in manifest_data:
            step_function_input['studyId'] = manifest_data['studyId']
        
        return step_function_input, workflow_id 