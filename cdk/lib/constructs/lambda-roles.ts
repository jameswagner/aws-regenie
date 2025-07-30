import * as cdk from 'aws-cdk-lib';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as batch from 'aws-cdk-lib/aws-batch';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';

/**
 * Interface for creating Lambda roles with standard properties
 */
export interface LambdaRoleProps {
  scope: cdk.Stack;
  dataBucket?: s3.Bucket;
  resultsBucket?: s3.Bucket;
  batchJobQueue?: batch.CfnJobQueue;
  workflowTable?: dynamodb.Table;
  jobStatusTable?: dynamodb.Table;
}

/**
 * Utility class for creating IAM roles for Lambda functions
 */
export class LambdaRoles {
  /**
   * Helper method to add CloudWatch Logs permissions to a role
   */
  private static addCloudWatchLogsPermissions(
    role: iam.Role, 
    functionNamePrefix: string,
    region: string,
    account: string
  ): void {
    role.addToPolicy(new iam.PolicyStatement({
      actions: [
        'logs:CreateLogGroup',
        'logs:CreateLogStream',
        'logs:PutLogEvents'
      ],
      resources: [
        `arn:aws:logs:${region}:${account}:log-group:/aws/lambda/${functionNamePrefix}*`,
      ]
    }));
  }

  /**
   * Helper method to create a basic Lambda execution role with VPC access
   */
  private static createBaseLambdaRole(scope: cdk.Stack, roleName: string): iam.Role {
    return new iam.Role(scope, roleName, {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaVPCAccessExecutionRole'),
      ],
    });
  }

  /**
   * Helper method to add S3 read permissions
   */
  private static addS3ReadPermissions(role: iam.Role, bucket: s3.Bucket): void {
    bucket.grantRead(role);
  }

  /**
   * Helper method to add S3 read-write permissions
   */
  private static addS3ReadWritePermissions(role: iam.Role, bucket: s3.Bucket): void {
    bucket.grantReadWrite(role);
  }

  /**
   * Helper method to add FSx permissions
   */
  private static addFsxPermissions(role: iam.Role, region: string, account: string, actions: string[]): void {
    role.addToPolicy(new iam.PolicyStatement({
      actions,
      resources: [
        `arn:aws:fsx:${region}:${account}:file-system/*`,
        `arn:aws:fsx:${region}:${account}:association/*`
      ]
    }));
  }

  /**
   * Helper method to add batch job permissions
   */
  private static addBatchPermissions(role: iam.Role, batchJobQueue: batch.CfnJobQueue, region: string, account: string, actions: string[]): void {
    role.addToPolicy(new iam.PolicyStatement({
      actions,
      resources: [
        batchJobQueue.attrJobQueueArn,
        `arn:aws:batch:${region}:${account}:job-definition/*`,
        `arn:aws:batch:${region}:${account}:job/*`
      ]
    }));
  }

  /**
   * Helper method to add DynamoDB permissions to a specific table
   */
  private static addDynamoDBPermissions(role: iam.Role, table: dynamodb.Table, actions: string[]): void {
    role.addToPolicy(new iam.PolicyStatement({
      actions,
      resources: [
        table.tableArn,
        `${table.tableArn}/index/*`
      ]
    }));
  }

  /**
   * Helper method to add DynamoDB full permissions (read/write) to a specific table
   */
  private static addDynamoDBFullPermissions(role: iam.Role, table: dynamodb.Table): void {
    this.addDynamoDBPermissions(role, table, [
      'dynamodb:GetItem',
      'dynamodb:PutItem',
      'dynamodb:UpdateItem',
      'dynamodb:DeleteItem'
    ]);
  }

  /**
   * Helper method to add batch job queue permissions
   */
  private static addBatchJobQueuePermissions(role: iam.Role, batchJobQueue: batch.CfnJobQueue): void {
    this.addBatchPermissions(role, batchJobQueue, role.env.region, role.env.account, [
      'batch:DescribeJobs',
      'batch:ListJobs',
      'batch:DescribeJobQueues',
      'batch:DescribeJobDefinitions'
    ]);
  }

  /**
   * Create role for workflow initialization Lambda with S3 and DynamoDB permissions
   */
  public static createWorkflowInitRole(props: LambdaRoleProps): iam.Role {
    const { scope, dataBucket, resultsBucket, workflowTable, jobStatusTable } = props;
    
    const role = this.createBaseLambdaRole(scope, 'WorkflowInitExecutionRole');

    // Add S3 permissions
    if (dataBucket) this.addS3ReadPermissions(role, dataBucket);
    if (resultsBucket) this.addS3ReadWritePermissions(role, resultsBucket);
    
    // Add DynamoDB permissions for all tables
    if (workflowTable) this.addDynamoDBFullPermissions(role, workflowTable);
    if (jobStatusTable) this.addDynamoDBFullPermissions(role, jobStatusTable);
    
    // Add CloudWatch Logs permissions
    this.addCloudWatchLogsPermissions(role, 'WorkflowInitFunction', scope.region, scope.account);
    
    return role;
  }

  /**
   * Creates IAM role for association handler function
   */
  public static createAssociationHandlerRole(props: LambdaRoleProps): iam.Role {
    const { scope, dataBucket, workflowTable } = props;
    
    const role = this.createBaseLambdaRole(scope, 'AssociationHandlerExecutionRole');

    // Add specific FSx permissions
    this.addFsxPermissions(role, scope.region, scope.account, [
      'fsx:DescribeFileSystems', 
      'fsx:DescribeDataRepositoryAssociations',
      'fsx:CreateDataRepositoryAssociation',
      'fsx:DeleteDataRepositoryAssociation'
    ]);

    // Add S3 permissions
    if (dataBucket) {
      role.addToPolicy(new iam.PolicyStatement({
        actions: [
          's3:GetObject',
          's3:GetObjectVersion',
          's3:GetObjectTagging',
          's3:ListBucket',
          's3:ListBucketVersions',
          's3:PutObject'
        ],
        resources: [
          `arn:aws:s3:::${dataBucket.bucketName}`,
          `arn:aws:s3:::${dataBucket.bucketName}/*`
        ]
      }));
    }

    // Add permission to access workflow table for updating status
    if (workflowTable) {
      this.addDynamoDBPermissions(role, workflowTable, [
        'dynamodb:GetItem',
        'dynamodb:UpdateItem'
      ]);
    }

    // Add permission to create service-linked roles for FSx
    role.addToPolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: [
        'iam:CreateServiceLinkedRole',
        'iam:AttachRolePolicy',
        'iam:PutRolePolicy'
      ],
      resources: ['arn:aws:iam::*:role/aws-service-role/s3.data-source.lustre.fsx.amazonaws.com/*']
    }));

    // Add CloudWatch Logs permissions
    this.addCloudWatchLogsPermissions(role, 'AssociationHandlerFunction', scope.region, scope.account);
    
    return role;
  }

  /**
   * Create role for Job Calculator Lambda with DynamoDB, S3, and Batch permissions
   */
  public static createJobCalculatorRole(props: LambdaRoleProps): iam.Role {
    const { scope, dataBucket, resultsBucket, workflowTable, jobStatusTable, batchJobQueue } = props;
    
    const role = this.createBaseLambdaRole(scope, 'JobCalculatorExecutionRole');

    // Add S3 permissions for data bucket (for chromosome detection)
    if (dataBucket) this.addS3ReadPermissions(role, dataBucket);
    if (resultsBucket) this.addS3ReadWritePermissions(role, resultsBucket);
    
    // Add DynamoDB permissions for all tables
    if (workflowTable) this.addDynamoDBFullPermissions(role, workflowTable);
    if (jobStatusTable) this.addDynamoDBFullPermissions(role, jobStatusTable);
    
    // Add Batch permissions to query job queues and descriptions
    if (batchJobQueue) {
      this.addBatchJobQueuePermissions(role, batchJobQueue);
    }
    
    return role;
  }

  /**
   * Creates IAM role for error handler function
   */
  public static createErrorHandlerRole(props: LambdaRoleProps): iam.Role {
    const { scope, workflowTable, jobStatusTable, batchJobQueue } = props;
    
    const role = this.createBaseLambdaRole(scope, 'ErrorHandlerExecutionRole');

    // Add DynamoDB permissions
    if (workflowTable) workflowTable.grantReadWriteData(role);
    if (jobStatusTable) jobStatusTable.grantReadWriteData(role);
    
    // Add batch permissions
    if (batchJobQueue) {
      this.addBatchPermissions(role, batchJobQueue, scope.region, scope.account, [
        'batch:DescribeJobs',
        'batch:ListJobs'
      ]);
    }

    // Add CloudWatch Logs permissions
    this.addCloudWatchLogsPermissions(role, 'ErrorHandlerFunction', scope.region, scope.account);
    
    return role;
  }

  /**
   * Create role for Success Handler Lambda with DynamoDB permissions
   */
  public static createSuccessHandlerRole(props: LambdaRoleProps): iam.Role {
    const { scope, resultsBucket, workflowTable, jobStatusTable } = props;
    
    const role = this.createBaseLambdaRole(scope, 'SuccessHandlerExecutionRole');
    
    // Add S3 read permissions for results bucket
    if (resultsBucket) this.addS3ReadPermissions(role, resultsBucket);
    
    // Add DynamoDB permissions for all tables
    if (workflowTable) this.addDynamoDBFullPermissions(role, workflowTable);
    if (jobStatusTable) this.addDynamoDBFullPermissions(role, jobStatusTable);
    
    return role;
  }

  /**
   * Creates IAM role for command parser function
   */
  public static createCommandParserRole(props: LambdaRoleProps): iam.Role {
    const { scope } = props;
    
    const role = this.createBaseLambdaRole(scope, 'CommandParserExecutionRole');

    // Add batch permissions
    role.addToPolicy(new iam.PolicyStatement({
      actions: [
        'batch:DescribeJobDefinitions',
      ],
      resources: [
        `arn:aws:batch:${scope.region}:${scope.account}:job-definition/*`
      ],
    }));

    // Add CloudWatch Logs permissions
    this.addCloudWatchLogsPermissions(role, 'CommandParserFunction', scope.region, scope.account);
    
    return role;
  }

  /**
   * Creates IAM role for cleanup related functions
   */
  public static createCleanupRole(props: LambdaRoleProps, roleName: string): iam.Role {
    const { scope, workflowTable, jobStatusTable } = props;
    
    const role = this.createBaseLambdaRole(scope, roleName);

    // Add DynamoDB permissions
    if (workflowTable) workflowTable.grantReadWriteData(role);
    if (jobStatusTable) jobStatusTable.grantReadWriteData(role);
    
    // Add CloudWatch Logs permissions
    this.addCloudWatchLogsPermissions(role, roleName.replace('ExecutionRole', ''), scope.region, scope.account);
    
    return role;
  }

  /**
   * Creates IAM role for running executions check function
   */
  public static createRunningExecutionsRole(props: LambdaRoleProps): iam.Role {
    const { scope } = props;
    
    const role = this.createCleanupRole(props, 'CheckRunningExecutionsExecutionRole');
    
    // Add Step Functions permissions to check running executions
    role.addToPolicy(new iam.PolicyStatement({
      actions: [
        'states:ListExecutions',
        'states:DescribeExecution',
      ],
      resources: [
        `arn:aws:states:${scope.region}:${scope.account}:stateMachine:*`,
        `arn:aws:states:${scope.region}:${scope.account}:execution:*:*`
      ]
    }));
    
    return role;
  }

  /**
   * Creates IAM role for delete association function
   */
  public static createDeleteAssociationRole(props: LambdaRoleProps): iam.Role {
    const { scope } = props;
    
    const role = this.createBaseLambdaRole(scope, 'DeleteAssociationExecutionRole');

    // Add FSx permissions
    this.addFsxPermissions(role, scope.region, scope.account, [
      'fsx:DescribeFileSystems', 
      'fsx:DescribeDataRepositoryAssociations',
      'fsx:DeleteDataRepositoryAssociation'
    ]);

    // Add CloudWatch Logs permissions
    this.addCloudWatchLogsPermissions(role, 'DeleteAssociationFunction', scope.region, scope.account);
    
    return role;
  }

  /**
   * Creates IAM role for FSx association creator function
   */
  public static createFsxAssociationCreatorRole(props: LambdaRoleProps): iam.Role {
    const { scope, dataBucket } = props;
    
    const role = this.createBaseLambdaRole(scope, 'FsxAssociationCreatorExecutionRole');

    // Add specific FSx permissions
    this.addFsxPermissions(role, scope.region, scope.account, [
      'fsx:DescribeFileSystems', 
      'fsx:DescribeDataRepositoryAssociations',
      'fsx:CreateDataRepositoryAssociation'
    ]);

    // Add S3 permissions
    if (dataBucket) {
      role.addToPolicy(new iam.PolicyStatement({
        actions: [
          's3:GetObject',
          's3:GetObjectVersion',
          's3:GetObjectTagging',
          's3:ListBucket'
        ],
        resources: [
          `arn:aws:s3:::${dataBucket.bucketName}`,
          `arn:aws:s3:::${dataBucket.bucketName}/*`
        ]
      }));
    }

    // Add permission to create service-linked roles for FSx
    role.addToPolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: [
        'iam:CreateServiceLinkedRole'
      ],
      resources: ['arn:aws:iam::*:role/aws-service-role/s3.data-source.lustre.fsx.amazonaws.com/*']
    }));

    // Add CloudWatch Logs permissions
    this.addCloudWatchLogsPermissions(role, 'FsxAssociationCreatorFunction', scope.region, scope.account);
    
    return role;
  }
} 