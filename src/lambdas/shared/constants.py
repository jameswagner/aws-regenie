"""
Constants module for AWS GWAS workflow lambdas.
Centralizes magic strings, numbers, and configuration values.
"""

# =============================================================================
# FILE FORMATS AND EXTENSIONS
# =============================================================================

# Supported genomic data formats
class GenomicFormats:
    BED = "bed"
    PGEN = "pgen" 
    BGEN = "bgen"
    
    ALL_FORMATS = [BED, PGEN, BGEN]


# File extensions for each format
class FileExtensions:
    # BED format files
    BED = ".bed"
    BIM = ".bim"
    FAM = ".fam"
    
    # PGEN format files  
    PGEN = ".pgen"
    PVAR = ".pvar"
    PSAM = ".psam"
    
    # BGEN format files
    BGEN = ".bgen"
    SAMPLE = ".sample"
    
    # Manifest files
    MANIFEST_JSON = ".manifest.json"


# =============================================================================
# MANIFEST PROCESSING
# =============================================================================

class ManifestConstants:
    # Manifest file patterns
    MANIFEST_FILENAME = "manifest.json"
    MANIFEST_FILE_SUFFIX = ".manifest.json"
    
    # Content type
    JSON_CONTENT_TYPE = "application/json"
    
    # Encoding
    UTF8_ENCODING = "utf-8"
    
    # Default data prefix
    DEFAULT_DATA_PREFIX = "genomics"


# =============================================================================
# FILE FORMAT MAPPINGS
# =============================================================================

class FormatFileMapping:
    """Maps file formats to their required file extensions"""
    
    FORMAT_EXTENSIONS = {
        GenomicFormats.BED: [FileExtensions.BED, FileExtensions.BIM, FileExtensions.FAM],
        GenomicFormats.PGEN: [FileExtensions.PGEN, FileExtensions.PVAR, FileExtensions.PSAM],
        GenomicFormats.BGEN: [FileExtensions.BGEN, FileExtensions.SAMPLE]
    }
    
    # Extensions for chromosome detection
    CHROMOSOME_DETECTION_EXTENSIONS = {
        GenomicFormats.BED: FileExtensions.BIM,
        GenomicFormats.PGEN: FileExtensions.PVAR
    }


# =============================================================================
# COMMAND PARSING
# =============================================================================

class CommandConstants:
    # Shell command components
    SHELL_EXECUTABLE = "/bin/bash"
    SHELL_FLAG = "-c"
    
    # Default job definition
    DEFAULT_JOB_DEFINITION = "GwasRegenieJobDefinitionRef"
    
    # Parameter keys
    JOB_ID_KEY = "JobId"
    JOB_QUEUE_KEY = "JobQueue"
    JOB_DEFINITION_KEY = "JobDefinition"
    COMMAND_KEY = "Command"
    
    # Step information keys
    STEP_NUMBER_KEY = "StepNumber"
    START_STEP_KEY = "StartStep"
    
    # FSx parameter keys
    USE_FSX_KEY = "UseFsx"
    FSX_PATH_KEY = "FsxPath"
    
    # Step 2 specific keys
    CHROMOSOME_NUMBER_KEY = "ChromosomeNumber"
    PRED_LIST_PATH_KEY = "PredListPath"
    OUTPUT_PREFIX_KEY = "OutputPrefix"


# =============================================================================
# WORKFLOW AND JOB STATUS
# =============================================================================

class WorkflowStatus:
    INITIALIZED = "INITIALIZED"
    CALCULATING_JOBS = "CALCULATING_JOBS"
    JOBS_CALCULATED = "JOBS_CALCULATED"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    COMPLETED_WITH_ERRORS = "COMPLETED_WITH_ERRORS"
    FAILED = "FAILED"


class JobStatus:
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


# =============================================================================
# JOB CALCULATION
# =============================================================================

class JobConstants:
    # Default parameter values
    DEFAULT_TRAIT_TYPE = "qt"
    DEFAULT_BLOCK_SIZE = 1000
    DEFAULT_MIN_MAC = 5
    DEFAULT_THREADS = 8
    DEFAULT_CV_FOLDS = 5
    DEFAULT_LOWMEM = True
    DEFAULT_OUTPUT_PREFIX = "results"
    DEFAULT_GZ_OUTPUT = True
    
    # Job ID patterns
    STEP1_JOB_PATTERN = "{workflow_id}-step1"
    STEP2_JOB_PATTERN = "{workflow_id}-step2-chr{chrom}"
    
    # Prediction file pattern
    PREDICTION_FILE_PATTERN = "{out_prefix}_pred.list"
    
    # Default chromosomes (22 autosomes + sex chromosomes)
    DEFAULT_CHROMOSOMES = [str(i) for i in range(1, 23)] + ['X', 'Y']
    
    # Parameter keys
    WORKFLOW_ID_KEY = "workflowId"
    JOB_ID_KEY = "jobId"
    
    # Step numbers
    STEP_1 = 1
    STEP_2 = 2


# =============================================================================
# DYNAMODB UTILITIES
# =============================================================================

class DynamoDBConstants:
    # Table attribute names
    STATUS_ATTR = "status"
    UPDATED_AT_ATTR = "updatedAt"
    ERROR_DETAIL_ATTR = "errorDetail"
    STEP_NUMBER_ATTR = "stepNumber"
    
    # Job statistics keys
    TOTAL_KEY = "total"
    COMPLETED_KEY = "completed"
    FAILED_KEY = "failed"
    PENDING_KEY = "pending"
    
    # DynamoDB expression placeholders
    STATUS_PLACEHOLDER = "#status"
    WORKFLOW_ID_PLACEHOLDER = ":workflowId"
    STATUS_VALUE_PLACEHOLDER = ":status"
    UPDATED_AT_PLACEHOLDER = ":updatedAt"
    ERROR_DETAIL_PLACEHOLDER = ":errorDetail"


# =============================================================================
# S3 UTILITIES
# =============================================================================

class S3Constants:
    # S3 URI patterns
    S3_URI_PATTERN = r'^s3://([^/]+)/(.*)$'
    S3_PREFIX = "s3://"
    
    # Path separators
    PATH_SEPARATOR = "/"
    
    # Error response keys
    ERROR_KEY = "Error"
    CODE_KEY = "Code"
    NOT_FOUND_CODE = "404"


# =============================================================================
# ENVIRONMENT VARIABLES
# =============================================================================

class EnvironmentVariables:
    # Bucket names
    DATA_BUCKET_NAME = "DATA_BUCKET_NAME"
    RESULTS_BUCKET_NAME = "RESULTS_BUCKET_NAME"
    
    # Table names
    WORKFLOW_TABLE_NAME = "WORKFLOW_TABLE_NAME"
    JOB_STATUS_TABLE_NAME = "JOB_STATUS_TABLE_NAME"
    
    # FSx and path variables
    FSX_FILESYSTEM_ID = "FSX_FILESYSTEM_ID"
    FSX_MOUNT_PATH = "FSX_MOUNT_PATH"
    DATA_PREFIX = "DATA_PREFIX"
    
    # Service configuration
    POWERTOOLS_SERVICE_NAME = "POWERTOOLS_SERVICE_NAME"
    LOG_LEVEL = "LOG_LEVEL"


# =============================================================================
# WORKFLOW INITIALIZATION
# =============================================================================

class WorkflowInitConstants:
    # Default user ID
    DEFAULT_USER_ID = "anonymous"
    
    # TTL calculation (30 days in seconds)
    TTL_DAYS = 30
    TTL_SECONDS = TTL_DAYS * 24 * 60 * 60
    
    # Default job statistics
    DEFAULT_JOB_STATS = {
        'total': 0,
        'completed': 0,
        'failed': 0,
        'pending': 0
    }


# =============================================================================
# ERROR HANDLING
# =============================================================================

class ErrorHandlerConstants:
    # Response status values
    NO_FAILURES = "NO_FAILURES"
    ERROR_STATUS = "ERROR"
    
    # Default error message
    UNKNOWN_ERROR = "Unknown error"
    
    # Response messages
    WORKFLOW_NOT_FOUND = "Workflow not found"
    WORKFLOW_COMPLETED_SUCCESSFULLY = "Workflow completed successfully"


# =============================================================================
# ERROR MESSAGES
# =============================================================================

class ErrorMessages:
    # Parameter validation errors
    MISSING_S3_PATH = "Either s3Path or datasetPath must be provided"
    MISSING_WORKFLOW_ID = "Missing required parameter: workflowId"
    MISSING_COMMAND = "Missing required parameter: Command"
    MISSING_ANALYSIS_SUBDIR = "Missing analysisSubdir in input"
    MISSING_PREDICTION_FILE = "When starting with step 2, a prediction file must be provided"
    EMPTY_COMMAND = "Command cannot be empty"
    CHR_AND_CHR_LIST_CONFLICT = "Cannot specify both 'chr' and 'chrList' parameters. Please use only one of these options."
    
    # File format errors
    MALFORMED_BIM = "Malformed BIM file: each line must have at least 6 fields."
    MALFORMED_PVAR = "Malformed PVAR file: each line must have at least 1 field."
    
    # Manifest errors
    INVALID_S3_PATH = "Invalid S3 path: {path}"
    MISSING_FILES = "Missing required files: {files}"
    DATASET_NOT_FOUND = "Dataset path not found: {path}"
    S3_ACCESS_ERROR = "Error accessing S3 bucket: {bucket}"
    
    # Job calculation errors
    UNSUPPORTED_FORMAT_FOR_CHROMOSOME_DETECTION = "Format {format} not supported for chromosome extraction, using defaults"
    
    # Error handling messages
    SKIPPING_JOB_WITHOUT_ID = "Skipping job without jobId: {job}"
    FAILED_TO_UPDATE_JOB = "Failed to update job {job_id}: {error}"
    ERROR_VALIDATING_WORKFLOW = "Error validating workflow {workflow_id}: {error}"
    ERROR_CALCULATING_JOB_STATS = "Error calculating job stats for workflow {workflow_id}: {error}" 