import * as cdk from 'aws-cdk-lib';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as iam from 'aws-cdk-lib/aws-iam';
import { Construct } from 'constructs';
import { DynamoDBTableFactory, GSIConfig } from '../constructs/dynamodb-table-factory';

/**
 * Database stack for the GWAS workflow application.
 * 
 * Creates and manages DynamoDB tables used to store workflow state, job status,
 * and FSx association tracking information.
 */
export class DatabaseStack extends cdk.Stack {
  // Public properties exposed to other stacks
  public readonly workflowTable: dynamodb.Table;
  public readonly jobStatusTable: dynamodb.Table;
  
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // Create the table factory
    const tableFactory = new DynamoDBTableFactory(this);

    // =========================================================================
    // Workflow Table - Primary tracking table for all workflow executions
    // =========================================================================
    
    // Define GSIs for workflow table
    const workflowGSIs: GSIConfig[] = [
      {
        // Index for querying workflows by status
        indexName: 'status-index',
        partitionKey: { name: 'status', type: dynamodb.AttributeType.STRING },
        sortKey: { name: 'updatedAt', type: dynamodb.AttributeType.STRING }
      },
      {
        // Index for querying by user
        indexName: 'user-index',
        partitionKey: { name: 'userId', type: dynamodb.AttributeType.STRING },
        sortKey: { name: 'createdAt', type: dynamodb.AttributeType.STRING }
      },
      {
        // Index for querying by study
        indexName: 'study-index',
        partitionKey: { name: 'studyId', type: dynamodb.AttributeType.STRING },
        sortKey: { name: 'createdAt', type: dynamodb.AttributeType.STRING }
      },
      {
        // Index for querying by creation time
        indexName: 'created-at-index',
        partitionKey: { name: 'createdAt', type: dynamodb.AttributeType.STRING }
      }
    ];
    
    // Create DynamoDB table for workflow tracking with workflowId as the partition key
    this.workflowTable = tableFactory.createTable('GwasWorkflowTable', {
      partitionKey: { name: 'workflowId', type: dynamodb.AttributeType.STRING },
      globalSecondaryIndexes: workflowGSIs
    });

    // =========================================================================
    // Job Status Table - Tracks individual batch jobs within a workflow
    // =========================================================================
    
    // Define GSIs for job status table
    const jobStatusGSIs: GSIConfig[] = [
      {
        // Index for querying by status
        indexName: 'status-index',
        partitionKey: { name: 'status', type: dynamodb.AttributeType.STRING },
        sortKey: { name: 'updatedAt', type: dynamodb.AttributeType.STRING }
      },
      {
        // Index for querying by batch job ID
        indexName: 'batchjob-index',
        partitionKey: { name: 'batchJobId', type: dynamodb.AttributeType.STRING }
      }
    ];
    
    // Create DynamoDB table for job status tracking with all GSIs
    this.jobStatusTable = tableFactory.createTable('GwasJobStatusTable', {
      partitionKey: { name: 'workflowId', type: dynamodb.AttributeType.STRING },
      sortKey: { name: 'jobId', type: dynamodb.AttributeType.STRING },
      globalSecondaryIndexes: jobStatusGSIs
    });


  }
} 