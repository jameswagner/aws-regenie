#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import { NetworkStack } from '../lib/stacks/network-stack';
import { StorageStack } from '../lib/stacks/storage-stack';
import { ComputeStack } from '../lib/stacks/compute-stack';
import { LambdaStack } from '../lib/stacks/lambda-stack';
import { WorkflowStack } from '../lib/stacks/workflow-stack';
import { DatabaseStack } from '../lib/stacks/database-stack';
import { QueueProcessingStack } from '../lib/stacks/queue-processing-stack';
import { MonitoringStack } from '../lib/stacks/monitoring-stack';

const app = new cdk.App();

// Get AWS environment from context or use default
const env = { 
  account: app.node.tryGetContext('account') || process.env.CDK_DEFAULT_ACCOUNT, 
  region: app.node.tryGetContext('region') || process.env.CDK_DEFAULT_REGION || 'us-east-1'
};

// Define tags for all resources
const tags = {
  Project: 'AWS-GWAS',
  Environment: app.node.tryGetContext('environment') || 'dev',
  Owner: 'GenomicsTeam'
};

// Define a common synthesizer with the qualifier matching bootstrap stack
const stackSynthesizer = new cdk.DefaultStackSynthesizer({
  qualifier: 'gwas'
});

// Create network stack
const networkStack = new NetworkStack(app, 'GwasNetworkStack', { 
  env,
  description: 'VPC and network infrastructure for GWAS workflow',
  synthesizer: stackSynthesizer
});

// Create storage stack
const storageStack = new StorageStack(app, 'GwasStorageStack', {
  env,
  vpc: networkStack.vpc,
  description: 'S3 and FSx storage for GWAS workflow with S3 data repository integration',
  synthesizer: stackSynthesizer
});

// Create database stack
const databaseStack = new DatabaseStack(app, 'GwasDatabaseStack', {
  env,
  description: 'DynamoDB tables for GWAS workflow tracking',
  synthesizer: stackSynthesizer
});

// Create compute stack
const computeStack = new ComputeStack(app, 'GwasComputeStack', {
  env,
  vpc: networkStack.vpc,
  fileSystem: storageStack.fileSystem,
  batchSecurityGroup: networkStack.batchSecurityGroup,
  dataBucket: storageStack.dataBucket,
  jobVcpus: app.node.tryGetContext('jobVcpus'),
  jobMemoryMiB: app.node.tryGetContext('jobMemoryMiB'),
  description: 'Batch and ECR infrastructure for GWAS workflow',
  synthesizer: stackSynthesizer
});

// Create Lambda stack
const lambdaStack = new LambdaStack(app, 'GwasLambdaStack', {
  env,
  vpc: networkStack.vpc,
  dataBucket: storageStack.dataBucket,
  resultsBucket: storageStack.resultsBucket,
  batchJobQueue: computeStack.batchJobQueue,
  workflowTable: databaseStack.workflowTable,
  jobStatusTable: databaseStack.jobStatusTable,
  fileSystem: storageStack.fileSystem,
  dataPrefix: 'genomics',
  fsxInputMountPath: storageStack.fsxInputMountPath,
  fsxOutputMountPath: storageStack.fsxOutputMountPath,
  description: 'Lambda functions for GWAS workflow',
  synthesizer: stackSynthesizer
});

// Create workflow stack
const workflowStack = new WorkflowStack(app, 'GwasWorkflowStack', {
  env,
  workflowInitFunction: lambdaStack.workflowInitFunction,
  jobCalculatorFunction: lambdaStack.jobCalculatorFunction,
  errorHandlerFunction: lambdaStack.errorHandlerFunction,
  commandParserFunction: lambdaStack.commandParserFunction,
  successHandlerFunction: lambdaStack.successHandlerFunction,
  batchJobQueue: computeStack.batchJobQueue,
  batchJobDefinition: computeStack.batchJobDefinition,
  dataBucket: storageStack.dataBucket,
  resultsBucket: storageStack.resultsBucket,
  description: 'Step Functions workflow for GWAS',
  synthesizer: stackSynthesizer
});

// Create queue processing stack - this is the stack for SQS -> Lambda -> Step Function
const queueProcessingStack = new QueueProcessingStack(app, 'GwasQueueProcessingStack', {
  env,
  vpc: networkStack.vpc,
  dataBucket: storageStack.dataBucket,
  resultsBucket: storageStack.resultsBucket,
  manifestNotificationTopic: storageStack.manifestNotificationTopic,
  stateMachine: workflowStack.stateMachine,
  description: 'SQS Queue Processing system for GWAS workflows',
  synthesizer: stackSynthesizer
});

// Get notification email from context if provided
const notificationEmail = app.node.tryGetContext('notificationEmail');

// Create monitoring stack for alarms and dashboards
const monitoringStack = new MonitoringStack(app, 'GwasMonitoringStack', {
  env,
  manifestDeadLetterQueue: queueProcessingStack.manifestDeadLetterQueue,
  manifestQueue: queueProcessingStack.manifestQueue,
  manifestProcessorFunction: queueProcessingStack.manifestProcessorFunction,
  manifestTriggerFunction: queueProcessingStack.manifestProcessorFunction,
  stateMachine: workflowStack.stateMachine,
  batchJobQueue: computeStack.batchJobQueue,
  notificationEmail: notificationEmail,
  description: 'CloudWatch alarms and dashboards for GWAS workflow monitoring',
  synthesizer: stackSynthesizer
});

// Apply common tags to all stacks
const allStacks = [
  networkStack,
  storageStack,
  databaseStack,
  computeStack,
  lambdaStack,
  workflowStack,
  queueProcessingStack,
  monitoringStack
];

allStacks.forEach(stack => {
  Object.entries(tags).forEach(([key, value]) => {
    cdk.Tags.of(stack).add(key, value as string);
  });
});

// Add dependencies
storageStack.addDependency(networkStack);
computeStack.addDependency(networkStack);
computeStack.addDependency(storageStack);
lambdaStack.addDependency(networkStack);
lambdaStack.addDependency(storageStack);
lambdaStack.addDependency(computeStack);
lambdaStack.addDependency(databaseStack);
workflowStack.addDependency(lambdaStack);
queueProcessingStack.addDependency(networkStack);
queueProcessingStack.addDependency(storageStack);
queueProcessingStack.addDependency(workflowStack);
monitoringStack.addDependency(queueProcessingStack);