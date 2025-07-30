import * as cdk from 'aws-cdk-lib';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as batch from 'aws-cdk-lib/aws-batch';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as fsx from 'aws-cdk-lib/aws-fsx';
import { Construct } from 'constructs';
import { LambdaFactory } from '../constructs/lambda-factory';
import { LambdaRoles } from '../constructs/lambda-roles';

interface LambdaStackProps extends cdk.StackProps {
  vpc: ec2.Vpc;
  dataBucket: s3.Bucket;
  resultsBucket: s3.Bucket;
  batchJobQueue: batch.CfnJobQueue;
  workflowTable: dynamodb.Table;
  jobStatusTable: dynamodb.Table;
  fileSystem: fsx.LustreFileSystem;
  dataPrefix: string;
  fsxInputMountPath: string;
  fsxOutputMountPath: string;
}

export class LambdaStack extends cdk.Stack {
  public readonly workflowInitFunction: lambda.Function;
  public readonly jobCalculatorFunction: lambda.Function;
  public readonly errorHandlerFunction: lambda.Function;
  public readonly commandParserFunction: lambda.Function;
  public readonly successHandlerFunction: lambda.Function;

  constructor(scope: Construct, id: string, props: LambdaStackProps) {
    super(scope, id, props);

    // Common environment variables for all lambdas
    const commonEnvVars = {
      WORKFLOW_TABLE_NAME: props.workflowTable.tableName,
      JOB_STATUS_TABLE_NAME: props.jobStatusTable.tableName,
      FSX_FILESYSTEM_ID: props.fileSystem.fileSystemId,
      FSX_MOUNT_PATH: '/mnt/fsx',
      DATA_PREFIX: props.dataPrefix,
    };

    // Environment variables for workflow_init and job_calculator (need mount paths + data bucket)
    const fullPathEnvVars = {
      ...commonEnvVars,
      FSX_INPUT_MOUNT_PATH: props.fsxInputMountPath,
      FSX_OUTPUT_MOUNT_PATH: props.fsxOutputMountPath,
      DATA_BUCKET_NAME: props.dataBucket.bucketName,
      RESULTS_BUCKET_NAME: props.resultsBucket.bucketName,
    };

    // Create Lambda functions with correct environment variables per matrix
    this.workflowInitFunction = LambdaFactory.createFunction({
      scope: this,
      functionName: 'WorkflowInitFunction',
      description: 'Initializes GWAS workflows and validates input data',
      lambdaModule: 'workflow_init',
      timeout: cdk.Duration.seconds(300),
      memorySize: 512,
      environment: fullPathEnvVars, // Gets: INPUT_MOUNT_PATH, RESULTS_MOUNT_PATH, DATA_BUCKET_NAME
      vpc: props.vpc,
      role: LambdaRoles.createWorkflowInitRole({
        scope: this,
        dataBucket: props.dataBucket,
        resultsBucket: props.resultsBucket,
        workflowTable: props.workflowTable,
        jobStatusTable: props.jobStatusTable
      })
    });

    this.jobCalculatorFunction = LambdaFactory.createFunction({
      scope: this,
      functionName: 'JobCalculatorFunction', 
      description: 'Calculates and creates batch jobs for GWAS analysis',
      lambdaModule: 'job_calculator',
      timeout: cdk.Duration.minutes(15),
      ephemeralStorageSize: cdk.Size.gibibytes(10),
      memorySize: 512,
      environment: fullPathEnvVars, // Gets: INPUT_MOUNT_PATH, RESULTS_MOUNT_PATH, DATA_BUCKET_NAME
      vpc: props.vpc,
      role: LambdaRoles.createJobCalculatorRole({
        scope: this,
        dataBucket: props.dataBucket,
        workflowTable: props.workflowTable,
        jobStatusTable: props.jobStatusTable,
        batchJobQueue: props.batchJobQueue
      })
    });

    this.commandParserFunction = LambdaFactory.createFunction({
      scope: this,
      functionName: 'CommandParserFunction',
      description: 'Parses and validates GWAS analysis commands',
      lambdaModule: 'command_parser',
      timeout: cdk.Duration.seconds(60),
      memorySize: 256,
      environment: commonEnvVars, // Gets: NONE of the mount paths or buckets
      vpc: props.vpc,
      role: LambdaRoles.createCommandParserRole({
        scope: this,
        batchJobQueue: props.batchJobQueue
      })
    });

    this.errorHandlerFunction = LambdaFactory.createFunction({
      scope: this,
      functionName: 'ErrorHandlerFunction',
      description: 'Handles workflow and job errors',
      lambdaModule: 'error_handler',
      timeout: cdk.Duration.seconds(180),
      memorySize: 256,
      environment: commonEnvVars, // Gets: NONE of the mount paths or buckets
      vpc: props.vpc,
      role: LambdaRoles.createErrorHandlerRole({
        scope: this,
        workflowTable: props.workflowTable,
        jobStatusTable: props.jobStatusTable,
        batchJobQueue: props.batchJobQueue
      })
    });

    this.successHandlerFunction = LambdaFactory.createFunction({
      scope: this,
      functionName: 'SuccessHandlerFunction',
      description: 'Handles successful workflow completion',
      lambdaModule: 'success_handler',
      timeout: cdk.Duration.seconds(60),
      memorySize: 256,
      environment: commonEnvVars, // Gets: NONE of the mount paths or buckets
      vpc: props.vpc,
      role: LambdaRoles.createSuccessHandlerRole({
        scope: this,
        workflowTable: props.workflowTable,
        jobStatusTable: props.jobStatusTable,
        resultsBucket: props.resultsBucket
      })
    });
  }
} 