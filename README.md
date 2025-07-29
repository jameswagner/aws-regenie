# AWS GWAS Workflow Automation

This project implements an automated genomics workflow system for GWAS (Genome-Wide Association Studies) using AWS services and the CDK (Cloud Development Kit) in TypeScript.

## Architecture Overview

The solution orchestrates genomics workflows (specifically regenie) using:

- **AWS Step Functions** for workflow orchestration with error handling
- **AWS Lambda** for serverless compute and workflow initialization
- **AWS Batch** for distributed processing of genomic data
- **Amazon FSx for Lustre** for high-performance file access
- **Amazon S3** for data storage
- **Amazon SQS** for reliable message processing
- **Amazon SNS** for notifications
- **Amazon DynamoDB** for workflow and study metadata tracking
- **Amazon CloudWatch** for monitoring and logging
- **Amazon ECR** for container image storage

## Data Flow Architecture

```mermaid
graph TB
    %% External Input
    subgraph "External Input"
        A[Genomic Data Files<br/>.bed/.bim/.fam<br/>.pgen/.pvar/.psam<br/>.bgen/.sample] --> B[Manifest File<br/>manifest.json]
        B --> C[S3 Upload]
    end

    %% Trigger System
    subgraph "Manifest Trigger System"
        C --> D[S3 Event Notification]
        D --> E[SNS Topic]
        E --> F[SQS Queue]
        F --> G[Manifest Trigger Lambda]
        G --> H{Manifest Validation}
        H -->|Valid| I[Start Step Functions]
        H -->|Invalid| J[Error Logging]
    end

    %% Step Functions Workflow
    subgraph "Step Functions State Machine"
        I --> K[Workflow Init Lambda]
        K --> L[Job Calculator Lambda]
        L --> M{Start Step?}
        
        M -->|Step 1| N[Step 1: Model Building]
        M -->|Step 2| O[Step 2: Association Testing]
        
        N --> P[Command Parser Lambda]
        P --> Q[AWS Batch Job<br/>Single Job]
        Q --> R[Prediction File<br/>step1_out_pred.list]
        R --> O
        
        O --> S[Command Parser Lambda]
        S --> T[AWS Batch Jobs<br/>Parallel by Chromosome]
        
        T --> U{All Jobs Complete?}
        U -->|Yes| V[Success Handler Lambda]
        U -->|No| W[Error Handler Lambda]
    end

    %% Data Storage
    subgraph "Data Storage"
        subgraph "S3 Storage"
            X[Input Data Bucket]
            Y[Results Bucket]
        end
        
        subgraph "FSx for Lustre"
            Z[High-Performance<br/>File System]
        end
        
        subgraph "DynamoDB"
            AA[Workflow Table]
            BB[Job Status Table]
        end
    end

    %% Batch Processing
    subgraph "AWS Batch Processing"
        subgraph "Step 1 Jobs"
            CC[Single Model Building Job<br/>Processes all chromosomes]
        end
        
        subgraph "Step 2 Jobs"
            DD[Chromosome 1 Job]
            EE[Chromosome 2 Job]
            FF[Chromosome N Job]
        end
    end

    %% Monitoring
    subgraph "Monitoring & Notifications"
        GG[CloudWatch Alarms]
        HH[SNS Notifications]
        II[CloudWatch Dashboard]
    end

    %% Connections
    K -.-> AA
    L -.-> AA
    Q -.-> Z
    T -.-> Z
    V -.-> AA
    V -.-> BB
    W -.-> AA
    W -.-> BB
    
    X -.-> Z
    Z -.-> Y
    
    Q -.-> GG
    T -.-> GG
    GG -.-> HH
    
    %% Styling
    classDef lambda fill:#ff9999,stroke:#333,stroke-width:2px
    classDef batch fill:#99ccff,stroke:#333,stroke-width:2px
    classDef storage fill:#99ff99,stroke:#333,stroke-width:2px
    classDef monitoring fill:#ffcc99,stroke:#333,stroke-width:2px
    classDef external fill:#cccccc,stroke:#333,stroke-width:2px
    
    class G,K,L,P,S,V,W lambda
    class Q,T,CC,DD,EE,FF batch
    class X,Y,Z,AA,BB storage
    class GG,HH,II monitoring
    class A,B,C external
```

### Key Data Flow Steps:

1. **Data Upload & Trigger**: Genomic data files and manifest are uploaded to S3, triggering the workflow
2. **Manifest Processing**: S3 event → SNS → SQS → Lambda validates manifest and starts Step Functions
3. **Workflow Initialization**: Creates workflow record in DynamoDB and validates parameters
4. **Job Calculation**: Analyzes input data to determine required Batch jobs and chromosomes
5. **Step 1 Processing**: Single Batch job builds prediction models using all genomic data
6. **Step 2 Processing**: Parallel Batch jobs perform association testing by chromosome
7. **Results Storage**: Outputs stored in S3 results bucket and FSx for Lustre
8. **Status Tracking**: All workflow and job statuses tracked in DynamoDB tables
9. **Monitoring**: CloudWatch provides real-time monitoring and alerting

### Data Movement Patterns:

- **S3 → FSx**: Genomic data automatically imported to high-performance filesystem
- **FSx → Batch**: Jobs read from and write to FSx for optimal performance
- **Batch → S3**: Results automatically exported back to S3 for persistence
- **Lambda → DynamoDB**: All workflow metadata and status updates
- **Step Functions → Batch**: Orchestration with .sync integration for job monitoring

## REGENIE Workflow Implementation

The workflow implements a GWAS analysis pipeline using the REGENIE software, which operates in two primary steps:

### Direct FSx for Lustre and S3 Integration
- The workflow uses a persistent FSx for Lustre filesystem for high-performance access
- Instead of creating data repository associations dynamically, the FSx filesystem is directly integrated with S3
- The StorageStack configures the FSx for Lustre file system with auto-import settings for the S3 data bucket
- This approach simplifies the architecture and eliminates potential circular dependencies

### Manifest-Based Workflow Trigger System
- The system automatically starts GWAS workflows when manifest files are uploaded to S3
- S3 bucket sends notifications to an SNS topic when JSON files are uploaded
- The SNS topic forwards messages to an SQS queue for reliable processing
- A Lambda function consumes SQS messages, validates manifest files, and triggers the Step Functions workflow
- The system filters specifically for files named `manifest.json` or ending with `.manifest.json`

### Step 1: Model Building
- Single job that processes all input data to build prediction models
- Outputs a prediction file (e.g., `step1_out_pred.list`) with model coefficients
- Required for accurate association testing in Step 2
- Runs as a single AWS Batch job (no parallelization)

### Step 2: Association Testing
- Multiple parallel jobs split by chromosome for efficient processing
- Each chromosome analysis runs independently as a separate AWS Batch job
- References the prediction file created in Step 1
- Outputs chromosome-specific results for downstream analysis

### Dynamic Chromosome Detection
- The workflow automatically detects chromosomes from input BIM files
- Supports non-human organisms with different chromosome structures
- No hardcoded chromosome assumptions (vs. traditional 22 autosomes + X, Y approach)

### Step Functions State Machine
The workflow state machine orchestrates the entire process:

1. **Workflow Initialization**: Sets up workflow ID and parameters
2. **Job Calculation**: Determines required jobs based on the input dataset
   - For Step 1: Creates a single model-building job
   - For Step 2: Creates multiple chromosome-specific jobs
3. **Step Selection**: Determines whether to run Step 1 + Step 2 or only Step 2
   - When `startStep=1`: Runs both steps sequentially, ensuring Step 2 has access to Step 1 outputs
   - When `startStep=2`: Skips Step 1, assumes prediction files already exist
4. **Job Submission and Monitoring**: Submits appropriate Batch jobs and tracks execution
5. **Results Processing**: Handles successful completion via Success Handler Lambda
   - Updates workflow status to COMPLETED in DynamoDB
   - Stores final job statistics and completion metadata
   - Provides results location for downstream processing

### Parameter Flow
- **Step 1 to Step 2 Connection**: The prediction file path (`*_pred.list`) is automatically passed from Step 1 outputs to Step 2 jobs
- **Covariates Handling**: Covariates are optional and passed correctly to both steps if specified
- **Command Building**: The workflow generates complete REGENIE commands with all required parameters for both steps

### Error Handling
- Comprehensive job failure detection
- Structured error reporting to DynamoDB
- Automatic workflow status updates on both success and failure

### Shared Utilities
- **DynamoDB Utilities** (`lambdas/shared/dynamodb_utils.py`): Reusable functions for workflow and job status management
- **S3 Utilities** (`lambdas/shared/s3_utils.py`): Common S3 operations like file existence checks and URI parsing

## Project Goals

1. Automate regenie workflows for GWAS studies
2. Enable scientists to focus on research rather than infrastructure management
3. Provide scalable compute resources for genomic analysis
4. Implement comprehensive error handling and monitoring
5. Create a system that can be extended to other genomic workflows

## Key Features

### Main Workflow Components

- **Persistent FSx for Lustre Filesystem**: Direct S3 integration with auto-import capabilities
- **Manifest-Based Trigger System**: Workflow automatically starts when manifest files are uploaded to S3
- **Dynamic Command Generation**: Commands for regenie steps are built at runtime based on input parameters
- **Parallel Job Processing**: AWS Batch is used to scale out computational steps horizontally
- **Error Handling**: Comprehensive error detection, reporting, and recovery
- **Study Tracking**: Full study metadata and execution tracking via DynamoDB

### Stack Architecture

The infrastructure is organized into eight well-defined stacks that deploy together to create the complete GWAS workflow system:

1. **NetworkStack**: 
   - VPC with public, private, and isolated subnets across 2 availability zones
   - Security groups for FSx and Batch communication with proper Lustre protocol ports (988, 1018-1023)
   - NAT Gateway for outbound internet access from private subnets

2. **StorageStack**: 
   - S3 buckets for genomic data and analysis results with lifecycle policies
   - FSx for Lustre filesystem with direct S3 integration and auto-import capabilities
   - SNS topic for manifest file upload notifications

3. **DatabaseStack**:
   - DynamoDB WorkflowTable for tracking workflow executions and status
   - DynamoDB JobStatusTable for tracking individual batch job progress
   - Global Secondary Indexes for querying by status, user, study, and creation time

4. **ComputeStack**:
   - AWS Batch compute environments and job queues for scalable genomics processing
   - Job definitions for regenie container execution
   - ECR repository for storing the regenie Docker image

5. **LambdaStack**:
   - Workflow initialization function for parameter validation and setup
   - Job calculator function for determining required batch jobs from input data
   - Command parser function for building regenie commands dynamically
   - Success handler function for workflow completion management
   - Error handler function for workflow failure management
   - All functions with properly scoped IAM roles and VPC integration

6. **WorkflowStack**:
   - Step Functions state machine for orchestrating the complete GWAS analysis pipeline
   - IAM roles with least-privilege permissions for state machine execution
   - Integration with Batch for .sync execution patterns

7. **QueueProcessingStack**:
   - SQS queue that subscribes to the manifest notification SNS topic
   - Dead-letter queue for failed message processing with configurable retry policies
   - Manifest processor Lambda function that validates manifest files and triggers workflows
   - Workflow notification topic for status updates

8. **MonitoringStack**:
   - CloudWatch alarms for Lambda function errors, Step Functions failures, and Batch job failures
   - Comprehensive dashboard showing critical workflow metrics
   - SNS topic for alarm notifications with optional email subscriptions
   - Custom metrics and monitoring for all workflow components

### IAM Permissions

The solution follows least-privilege principles with dedicated IAM roles:

- Each Lambda function has its own IAM role with specific permissions for its tasks
- Permissions are scoped to specific resources and actions
- The manifest processor Lambda has permissions to read from S3 and start Step Functions executions

### Workflow Execution Logs

Regenie commands and execution details can be found in:

- **Lambda Logs**: Available in CloudWatch for each Lambda function
- **Step Functions Execution History**: Shows the workflow progression and state transitions 
- **AWS Batch Job Logs**: Contains detailed regenie command outputs and errors

### Database Schema

The system uses two core DynamoDB tables to track genomics workflows:

1. **WorkflowTable**: Records workflow executions with runtime parameters and execution status
2. **JobStatusTable**: Tracks individual batch job statuses within workflows

**Future Enhancements:** Additional tables planned for the system include StudyMetadataTable, DatasetTable, SamplesTable, ExecutionErrorsTable, and MetricsTable to enable multi-study organization, sample tracking, and performance metrics collection.

### Cleanup Workflow

A separate state machine handles cleanup of resources:
1. Checks for pending failures in DynamoDB
2. Verifies if there are running Step Functions executions
3. Makes decisions about cleaning up resources
4. Updates workflow status based on execution results

## Lambda Functions

The following Lambda functions are used in the workflow:

1. **Manifest Trigger** (`manifest_trigger`): Processes SQS messages from manifest file uploads and validates manifest files before initiating Step Functions workflows.

2. **Workflow Initialization** (`workflow_init`): Initializes the workflow, validates input parameters, and sets up the workflow execution context.

3. **Job Calculator** (`job_calculator`): Analyzes input genomic data to calculate and determine the required batch jobs based on chromosomes and analysis parameters.

4. **Command Parser** (`command_parser`): Parses and formats regenie commands for AWS Batch execution.

5. **Success Handler** (`success_handler`): Processes successful workflow completion, updates final workflow status to COMPLETED in DynamoDB, and stores completion metadata.

6. **Error Handler** (`error_handler`): Handles workflow failures, updates job and workflow statuses in DynamoDB, and determines final workflow state based on failure patterns.



## Getting Started

### Prerequisites

- Node.js 14.x or later
- AWS CDK CLI
- AWS CLI configured with appropriate permissions
- Docker (for local testing)

### Installation

1. Clone this repository
```
git clone https://github.com/your-org/aws-gwas.git
cd aws-gwas
```

2. Install dependencies
```
npm install
```

3. Install CDK dependencies
```
cd cdk
npm install
cd ..
```

4. Bootstrap CDK (if not already done)
```
cd cdk
cdk bootstrap
```

5. Deploy all infrastructure stacks
```
cdk deploy --all
```

To receive email notifications for workflow alarms, you can optionally provide an email address:
```
cdk deploy --all --context notificationEmail=your-email@example.com
```

This will deploy all 8 stacks in the correct dependency order:
- GwasNetworkStack (VPC and networking)
- GwasStorageStack (S3 and FSx)
- GwasDatabaseStack (DynamoDB tables)
- GwasComputeStack (Batch and ECR)
- GwasLambdaStack (Lambda functions)
- GwasWorkflowStack (Step Functions)
- GwasQueueProcessingStack (SQS and manifest processing)
- GwasMonitoringStack (CloudWatch alarms and dashboard)

## Configuration Options

The GWAS workflow can be configured using CDK context parameters to optimize for your dataset size and budget:

### Batch Job Resources

Configure the vCPU and memory allocated to each genomics job:

```bash
# Small datasets (1K-10K samples) - Default settings
cdk deploy --all

# Medium datasets (10K-100K samples) - More resources  
cdk deploy --all \
  --context jobVcpus=8 \
  --context jobMemoryMiB=32768

# Large datasets (100K+ samples) - High-memory instances
cdk deploy --all \
  --context jobVcpus=16 \
  --context jobMemoryMiB=65536
```

### Email Notifications

Optionally receive email alerts for workflow failures:

```bash
cdk deploy --all --context notificationEmail=your-email@example.com
```

### Complete Configuration Example

```bash
cdk deploy --all \
  --context jobVcpus=8 \
  --context jobMemoryMiB=32768 \
  --context notificationEmail=genomics-team@example.com
```

### Resource Configuration Guidelines

| Dataset Size | Samples | Recommended vCPUs | Recommended Memory | Cost Impact |
|--------------|---------|-------------------|-------------------|-------------|
| Small        | 1K-10K  | 4 (default)       | 16 GB (default)   | Low         |
| Medium       | 10K-100K| 8                 | 32 GB             | 2x cost     |
| Large        | 100K+   | 16                | 64 GB             | 4x cost     |

**Note:** These settings apply to each chromosome job. With 24 human chromosomes running in parallel, total resource usage scales accordingly.

## Unique S3 Paths for Concurrent Workflows

The system generates unique S3 paths for each workflow run to prevent conflicts when multiple workflows run simultaneously:

- Each run of `upload_and_run.py` creates a uniquely timestamped S3 directory for data
- This ensures multiple parallel workflows don't interfere with each other
- Results are stored in separate, uniquely named paths

## Project Structure

```
aws-gwas/
├── cdk/                    # CDK infrastructure code
│   ├── bin/
│   │   └── cdk.ts          # CDK app entry point
│   ├── lib/                # CDK stack definitions
│   │   ├── constructs/     # Reusable CDK constructs
│   │   │   ├── dynamodb-table-factory.ts  # Factory for DynamoDB tables
│   │   │   ├── lambda-factory.ts  # Factory for Lambda functions
│   │   │   ├── lambda-roles.ts  # Role definitions for Lambda functions
│   │   │   └── regenie-job-definition.ts  # Batch job definition
│   │   └── stacks/         # Main infrastructure stacks
│   │       ├── network-stack.ts     # VPC and networking infrastructure
│   │       ├── storage-stack.ts     # S3, FSx and SNS notification topic
│   │       ├── database-stack.ts    # DynamoDB tables for workflow tracking
│   │       ├── compute-stack.ts     # Batch compute environment and ECR
│   │       ├── lambda-stack.ts      # Lambda functions for workflow processing
│   │       ├── workflow-stack.ts    # Step Functions workflow definition
│   │       ├── queue-processing-stack.ts  # SQS, Lambda for manifest processing
│   │       └── monitoring-stack.ts  # CloudWatch alarms and dashboards
│   ├── cdk.json            # CDK configuration
│   └── package.json        # Node.js dependencies
├── src/                    # Application code
│   └── lambdas/            # Lambda function code
│       ├── workflow_init/  # Workflow initialization
│       ├── job_calculator/ # Batch job calculation
│       ├── error_handler/  # Error handling
│       ├── command_parser/ # Command string parsing
│       └── manifest_trigger/ # Process manifest files and trigger workflows
├── tests/                  # Test suite
│   ├── lambdas/            # Lambda function tests
│   └── conftest.py         # Pytest configuration
├── docs/                   # Documentation
│   └── schemas/            # JSON schemas for manifest validation
├── regenie-example/        # Example genomic data files
├── scripts/                # Utility scripts
│   └── upload_and_run.py   # Script to upload data and trigger workflows
├── requirements.txt        # Python dependencies
├── pyproject.toml          # Python project configuration
├── pytest.ini             # Pytest configuration
└── README.md               # This file
```

## Recent Improvements

### Streamlined Database Structure

The database architecture uses two core tables optimized for the workflow tracking requirements:

1. **WorkflowTable**: Records workflow executions with runtime parameters and status
2. **JobStatusTable**: Tracks individual batch job statuses within workflows

This focused approach reduces complexity while maintaining all essential workflow tracking functionality. Both tables include Global Secondary Indexes for efficient querying by status, user, study, and creation time.

### Code Refactoring and Abstractions

#### DynamoDB Table Factory

A new `DynamoDBTableFactory` class has been implemented to standardize DynamoDB table creation:

- Provides consistent table settings (billing mode, removal policy, etc.)
- Simplifies Global Secondary Index (GSI) creation
- Standardizes output generation
- Enables declarative table and index definitions using configuration objects

```typescript
// Example of simplified table creation
const workflowTable = tableFactory.createTable('GwasWorkflowTable', {
  partitionKey: { name: 'workflowId', type: dynamodb.AttributeType.STRING },
  sortKey: { name: 'createdAt', type: dynamodb.AttributeType.STRING }
});

// Define GSIs with a declarative approach
const workflowGSIs: GSIConfig[] = [
  {
    indexName: 'status-index',
    partitionKey: { name: 'status', type: dynamodb.AttributeType.STRING },
    sortKey: { name: 'updatedAt', type: dynamodb.AttributeType.STRING }
  },
  // Additional indexes...
];

// Add all GSIs to the table
workflowGSIs.forEach(gsi => tableFactory.addGSI(workflowTable, gsi));
```

#### Lambda Role Improvements

The `LambdaRoles` class has been refactored to reduce duplication and improve maintainability:

- Created helper methods for common IAM permissions (S3, DynamoDB, Batch, FSx)
- Standardized role creation with base role patterns
- Simplified policy statement creation
- Reduced code duplication across different role types

#### Lambda Function Factory

The `LambdaFactory` provides standardized Lambda function creation:

- Ensures consistent configuration across all functions
- Centralizes best practices for Lambda deployment
- Simplifies function outputs for cross-stack references

### Architecture Improvements

- **Modular Stack Design**: Infrastructure organized into 8 focused stacks with clear separation of concerns
- **Reduced Cross-Stack Dependencies**: Explicit dependency management between stacks
- **Direct Parameter Passing**: Stack-to-stack parameter passing instead of CloudFormation exports for better maintainability
- **Improved Construct Organization**: Reusable constructs and factories for consistent resource creation
- **Enhanced Monitoring**: Dedicated monitoring stack with comprehensive alarms and dashboards
- **Network Isolation**: Dedicated network stack with proper security group configurations

These improvements make the codebase more maintainable, provide better observability, and enable easier debugging and troubleshooting of workflow issues.

## Using the Manifest-Based Trigger System

### How to Use

#### 1. Upload Data Files

First, upload your genomic data files to an S3 path:

- For PLINK BED format: Upload `.bed`, `.bim`, and `.fam` files
- For PLINK2 PGEN format: Upload `.pgen`, `.pvar`, and `.psam` files
- For BGEN format: Upload `.bgen` and `.sample` files

Also upload any phenotype files and covariate files as needed.

Example:
```
s3://your-data-bucket/experiments/experiment-123/
  ├── example.bed
  ├── example.bim
  ├── example.fam
  ├── phenotype_bin.txt
  └── covariates.txt
```

#### 2. Create a Manifest File

Create a JSON manifest file with the required information:

At minimum, the manifest must include:
- `experimentId` - A unique identifier for your experiment
- `s3Path` - The S3 path where your genomic data files are stored
- `inputData` - Information about your input files

Example minimal manifest:
```json
{
  "experimentId": "experiment-123",
  "s3Path": "s3://your-data-bucket/experiments/experiment-123/",
  "inputData": {
    "format": "bed",
    "filePrefix": "example"
  }
}
```

#### 3. Upload the Manifest File

Upload your manifest file to the same S3 location as your data files. The file should be named:
- `manifest.json` or 
- `anything.manifest.json` 

For example:
```
s3://your-data-bucket/experiments/experiment-123/manifest.json
```

#### 4. Automatic Workflow Triggering

When the manifest file is uploaded, the system will:

1. Detect the manifest upload via S3 event notifications
2. Validate the manifest contents
3. Check that all required data files exist
4. Start the Step Functions workflow

You'll receive notifications about the workflow status via SNS (if subscribed).

## Usage for Scientists

Scientists can initiate genomics workflows through:

1. Uploading a manifest file to S3 to automatically trigger the workflow
2. Direct interaction with Step Functions API via AWS Console
3. AWS CLI by running `start-execution` and passing a JSON input file
4. The provided `upload_and_run.py` script for quick testing with example data

### Example input parameters:

```json
{
  "studyId": "study-123456",
  "datasetId": "dataset-abcdef",
  "sampleSize": 1000,
  "batchSize": 100,
  "regenieVersion": "latest",
  "startStep": "1",
  "datasetPath": "s3://genomics-data/dataset1",
  "phenotype": "diabetes",
  "samples": [
    {
      "sampleId": "sample-001",
      "diabetes": "0",
      "gender": "F",
      "age": "45"
    },
    {
      "sampleId": "sample-002",
      "diabetes": "1",
      "gender": "M",
      "age": "52"
    }
  ],
  "sampleManifest": "s3://genomics-data/manifests/study-123456-samples.csv"
}
```

## Monitoring and Error Handling

- CloudWatch dashboards provide real-time metrics on workflow performance
- Failed jobs are logged to DynamoDB for easy access and management
- Error notifications can be configured via SNS or email
- The cleanup workflow automatically detects and handles pending failures
- Study status is updated automatically based on workflow execution results

## Data Model

### Workflow Table
- **workflowId** (partition key): Unique identifier for a workflow execution
- **createdAt** (sort key): Timestamp when the workflow was created
- **status**: Current status of the workflow (INITIALIZED, CALCULATING_JOBS, IN_PROGRESS, COMPLETED, etc.)
- **fsxPath**: Path in FSx filesystem for the workflow's data
- **parameters**: Analysis parameters for regenie
- **datasetId**: Associated dataset identifier
- **jobCount**: Total number of jobs in the workflow
- **jobStats**: Statistics about job completion status

### Job Status Table
- **workflowId** (partition key): Workflow execution identifier
- **jobId** (sort key): Batch job identifier
- **status**: Current status of the job (PENDING, RUNNING, COMPLETED, FAILED)
- **stepNumber**: The regenie step number (1 or 2)
- **command**: The command executed by the job
- **createdAt**: Timestamp when the job was created
- **updatedAt**: Timestamp when the job was last updated
- **chromosomeNumber**: For step 2 jobs, the chromosome being processed
- **errorDetail**: For failed jobs, details about the error



## Future Enhancements

The following tables are planned for future implementation:

### Study Metadata Table (TODO)
- Will store study information including principal investigators and cohort details
- Will enable organizing multiple workflows under research studies

### Dataset Table (TODO)
- Will track genomic datasets with metadata
- Will manage associations between studies and their datasets

### Samples Table (TODO)
- Will store individual sample information and phenotypes
- Will enable cohort management and participant tracking

### Execution Errors Table (TODO)
- Will provide detailed error tracking and resolution status
- Will support better failure analysis and debugging

### Metrics Table (TODO)
- Will collect performance metrics for workflows and jobs
- Will enable optimization of compute resources and cost analysis

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- AWS Genomics team for architecture guidance
- REGENIE project for the genomic analysis software

# GWAS Workflow Automation

This script automates the process of:
1. Uploading the regenie Docker image to Amazon ECR
2. Uploading example genomic files to Amazon S3
3. Triggering a GWAS analysis using AWS Step Functions

## Prerequisites

- Python 3.6+
- AWS CLI configured with appropriate permissions
- Docker installed locally
- boto3 Python package (`pip install boto3`)

## Installation

```bash
# Clone the repository (if applicable)
git clone <repository-url>
cd <repository-directory>

# Install dependencies
pip install boto3
```

## Usage

The script can automatically detect your AWS resources by looking up the CloudFormation stacks:

```bash
python upload_and_run.py
```

If you want to specify the resources manually, you can still do so:

```bash
python upload_and_run.py --bucket YOUR_S3_BUCKET_NAME --state-machine-arn YOUR_STATE_MACHINE_ARN
```

### Optional Arguments

- `--stack-prefix`: Prefix for CDK stack names (default: "Gwas")
- `--image-tag`: Regenie image tag to use (default: 'v3.0.1.gz')
- `--ecr-repo`: ECR repository name (if not specified, will be retrieved from CloudFormation)
- `--bucket`: S3 bucket name (if not specified, will be retrieved from CloudFormation)
- `--state-machine-arn`: ARN of the state machine (if not specified, will be retrieved from CloudFormation)
- `--dataset-prefix`: Prefix for S3 dataset (default: 'example-data')
- `--example-dir`: Directory with example files (default: './regenie-example')

## Examples

### With automatic resource detection:

```bash
python upload_and_run.py
```

### With custom stack prefix:

```bash
python upload_and_run.py --stack-prefix MyCustomPrefix
```

### With manual resource specification:

```bash
python upload_and_run.py \
  --bucket genomics-data-bucket \
  --state-machine-arn arn:aws:states:us-east-1:123456789012:stateMachine:GwasWorkflow \
  --image-tag v3.0.1.gz
```

## How it Works

1. The script automatically discovers AWS resources by querying CloudFormation stacks
2. It checks if the specified regenie Docker image exists in ECR
3. If not, it pulls the image from GitHub Container Registry and pushes it to ECR
4. Example genomic files are uploaded to the specified S3 bucket
5. The workflow is triggered using the Step Functions state machine

## Output

The script prints the execution ARN and a link to monitor the workflow in the AWS console.

## Testing

Run tests using pytest:

```bash
# Run all tests
python -m pytest

# Run specific test file
python -m pytest tests/lambdas/test_job_calculator.py

# Run with coverage
python -m pytest --cov=src tests/
```

## TODOs

### Chromosome Detection Improvements
- [ ] **Support all genomic formats**: Currently only BED/PLINK (.bim) format supported for chromosome detection
  - [ ] Add PGEN format (.pvar file parsing)
  - [ ] Add VCF format (header parsing) 
  - [ ] Add BGEN format support
- [ ] **Large file handling**: Lambda 10GB ephemeral storage limit will fail on large GWAS datasets
  - [ ] Implement Batch job for chromosome detection on files >8GB
  - [ ] Implement Fargate task as alternative to Batch
  - [ ] Auto-fallback from Lambda to Batch/Fargate based on file size
- [ ] **Compressed file support**: Handle .gz, .bz2 compressed genomic files properly
- [ ] **Fail-fast validation**: Remove hardcoded chromosome fallbacks, require successful detection

### Architecture Improvements  
- [ ] **Split Lambda 3**: Separate chromosome detection from job orchestration
- [ ] **Add retry logic**: Exponential backoff for S3 downloads and DynamoDB writes
- [ ] **FSx validation**: Verify FSx mount paths exist before job creation

*Goal: Quick production readiness improvements while documenting the larger architectural work needed for enterprise-scale genomics datasets.*