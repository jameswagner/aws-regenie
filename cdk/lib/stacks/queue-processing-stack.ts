import * as cdk from 'aws-cdk-lib';
import * as sns from 'aws-cdk-lib/aws-sns';
import * as sqs from 'aws-cdk-lib/aws-sqs';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as snss from 'aws-cdk-lib/aws-sns-subscriptions';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as sfn from 'aws-cdk-lib/aws-stepfunctions';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as s3 from 'aws-cdk-lib/aws-s3';
import { Construct } from 'constructs';
import { LambdaFactory } from '../constructs/lambda-factory';

interface QueueProcessingStackProps extends cdk.StackProps {
  manifestNotificationTopic: sns.Topic;
  vpc: ec2.Vpc;
  dataBucket: s3.Bucket;
  resultsBucket: s3.Bucket;
  stateMachine: sfn.StateMachine;
}

/**
 * Stack that sets up the SQS queue processing system for GWAS manifests
 * Processes SNS notifications from the StorageStack and triggers the Step Function
 */
export class QueueProcessingStack extends cdk.Stack {
  // Expose resources that might be needed by other stacks
  public readonly manifestQueue: sqs.Queue;
  public readonly manifestDeadLetterQueue: sqs.Queue;
  public readonly manifestProcessorFunction: lambda.Function;
  
  constructor(scope: Construct, id: string, props: QueueProcessingStackProps) {
    super(scope, id, props);


    // Create a Dead Letter Queue for failed SQS messages
    this.manifestDeadLetterQueue = new sqs.Queue(this, 'ManifestDeadLetterQueue', {
      retentionPeriod: cdk.Duration.days(14),
      visibilityTimeout: cdk.Duration.seconds(30),
      removalPolicy: cdk.RemovalPolicy.DESTROY
    });

    // Create an SQS Queue that subscribes to the SNS topic
    this.manifestQueue = new sqs.Queue(this, 'ManifestQueue', {
      visibilityTimeout: cdk.Duration.seconds(300), // 5 minutes to match Lambda timeout
      retentionPeriod: cdk.Duration.days(4),
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      deadLetterQueue: {
        queue: this.manifestDeadLetterQueue,
        maxReceiveCount: 5
      }
    });

    // Subscribe the SQS queue to the SNS topic
    props.manifestNotificationTopic.addSubscription(
      new snss.SqsSubscription(this.manifestQueue)
    );

    // Create Lambda execution role with appropriate permissions
    const processorLambdaRole = new iam.Role(this, 'ManifestProcessorLambdaRole', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole'),
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaVPCAccessExecutionRole')
      ]
    });

    // Add permissions to read from S3
    props.dataBucket.grantRead(processorLambdaRole);

    // Add permissions to start Step Functions executions
    processorLambdaRole.addToPolicy(new iam.PolicyStatement({
      actions: ['states:StartExecution'],
      resources: [props.stateMachine.stateMachineArn]
    }));

    // Note: Manifest trigger Lambda is defined here (not in LambdaStack) to avoid circular dependencies:
    // - This Lambda needs Step Functions permissions (depends on WorkflowStack)
    // - WorkflowStack needs other Lambdas (depends on LambdaStack)
    // - Defining manifest trigger in LambdaStack would create: LambdaStack → WorkflowStack → LambdaStack cycle
    // - By keeping it here, we maintain clean dependency flow: QueueProcessing → Lambda → Workflow

    // Create the Lambda function that will process SQS messages
    this.manifestProcessorFunction = LambdaFactory.createFunction({
      scope: this,
      functionName: 'ManifestProcessorFunction',
      description: 'Processes manifest files and triggers GWAS workflows',
      lambdaModule: 'manifest_trigger',
      timeout: cdk.Duration.seconds(300), // 5-minute timeout
      memorySize: 512,
      environment: {
        STATE_MACHINE_ARN: props.stateMachine.stateMachineArn,
        RESULTS_BUCKET_NAME: props.resultsBucket.bucketName
      },
      vpc: props.vpc,
      role: processorLambdaRole
    });
    
    // Grant SQS permissions to the Lambda
    this.manifestQueue.grantConsumeMessages(this.manifestProcessorFunction);

    // Create event source mapping from SQS to Lambda
    new lambda.EventSourceMapping(this, 'SqsEventSourceMapping', {
      target: this.manifestProcessorFunction,
      eventSourceArn: this.manifestQueue.queueArn,
      batchSize: 5, 
      maxBatchingWindow: cdk.Duration.seconds(30), // Wait up to 30 seconds to collect messages
      reportBatchItemFailures: true
    });


  }
} 