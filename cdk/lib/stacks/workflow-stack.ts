import * as cdk from 'aws-cdk-lib';
import * as sfn from 'aws-cdk-lib/aws-stepfunctions';
import * as tasks from 'aws-cdk-lib/aws-stepfunctions-tasks';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as batch from 'aws-cdk-lib/aws-batch';
import * as s3 from 'aws-cdk-lib/aws-s3';
import { Construct } from 'constructs';

interface WorkflowStackProps extends cdk.StackProps {
  workflowInitFunction: lambda.Function;
  jobCalculatorFunction: lambda.Function;
  errorHandlerFunction: lambda.Function;
  commandParserFunction: lambda.Function;
  successHandlerFunction: lambda.Function;
  batchJobQueue: batch.CfnJobQueue;
  batchJobDefinition: batch.CfnJobDefinition;
  dataBucket: s3.Bucket;
  resultsBucket: s3.Bucket;
}

export class WorkflowStack extends cdk.Stack {
  public readonly stateMachine: sfn.StateMachine;
  
  constructor(scope: Construct, id: string, props: WorkflowStackProps) {
    super(scope, id, props);

    // Create Step Functions state machine execution role
    const stateMachineRole = new iam.Role(this, 'StateMachineExecutionRole', {
      assumedBy: new iam.ServicePrincipal('states.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaRole'),
      ],
    });

    // Add permissions to invoke Lambda functions
    props.workflowInitFunction.grantInvoke(stateMachineRole);
    props.jobCalculatorFunction.grantInvoke(stateMachineRole);
    props.errorHandlerFunction.grantInvoke(stateMachineRole);
    props.commandParserFunction.grantInvoke(stateMachineRole);
    props.successHandlerFunction.grantInvoke(stateMachineRole);

    // Add permissions to manage Batch jobs with .sync integration pattern
    stateMachineRole.addToPolicy(new iam.PolicyStatement({
      actions: [
        'batch:SubmitJob',
        'batch:ListJobs',
        'batch:CancelJob',
        'batch:DescribeJobs',
        'batch:TerminateJob'
      ],
      resources: [
        props.batchJobQueue.attrJobQueueArn,
        `arn:${cdk.Aws.PARTITION}:batch:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:job-definition/*`,
        `arn:${cdk.Aws.PARTITION}:batch:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:job/*`
      ]
    }));
    
    // Add permissions for EventBridge to handle .sync integration
    stateMachineRole.addToPolicy(new iam.PolicyStatement({
      actions: [
        'events:PutTargets',
        'events:PutRule',
        'events:DescribeRule'
      ],
      resources: [
        `arn:aws:events:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:rule/StepFunctionsGetEventsForBatchJobsRule`
      ],
    }));

    // Add S3 bucket access permissions
    stateMachineRole.addToPolicy(new iam.PolicyStatement({
      actions: [
        's3:GetObject',
        's3:ListBucket',
        's3:GetBucketLocation',
      ],
      resources: [
        props.dataBucket.bucketArn,
        `${props.dataBucket.bucketArn}/*`,
      ],
    }));

    // Add S3 results bucket access permissions
    stateMachineRole.addToPolicy(new iam.PolicyStatement({
      actions: [
        's3:GetObject',
        's3:PutObject',
        's3:ListBucket',
        's3:GetBucketLocation',
      ],
      resources: [
        props.resultsBucket.bucketArn,
        `${props.resultsBucket.bucketArn}/*`,
      ],
    }));



    // Define the workflow states
    const workflow = this.createGwasWorkflow(props);

    // Create the state machine
    this.stateMachine = new sfn.StateMachine(this, 'GwasWorkflow', {
      definitionBody: sfn.DefinitionBody.fromChainable(workflow),
      role: stateMachineRole,
      timeout: cdk.Duration.hours(48),
      removalPolicy: cdk.RemovalPolicy.DESTROY
    });


  }
  
  private createGwasWorkflow(props: WorkflowStackProps): sfn.IChainable {
    // Workflow Initialization with Lambda
    const initializeWorkflow = new tasks.LambdaInvoke(this, 'InitializeWorkflow', {
      lambdaFunction: props.workflowInitFunction,
      outputPath: '$.Payload'
    });

    // Calculate batch jobs
    const calculateBatchJobs = new tasks.LambdaInvoke(this, 'CalculateBatchJobs', {
      lambdaFunction: props.jobCalculatorFunction,
      inputPath: '$',
      resultPath: '$.JobCalculation',
      payload: sfn.TaskInput.fromObject({
        // Send flat structure that Job Calculator expects
        "workflowId.$": "$.workflowId",
        "s3Path.$": "$.s3Path",
        "analysisSubdir.$": "$.dataBucketPrefix",              // Map dataBucketPrefix to analysisSubdir
        "inputData.$": "$.parameters.inputData",
        "analysisParams.$": "$.parameters.analysisParams", 
        "outputParams.$": "$.parameters.outputParams"
      })
    });
    
    // Step 1 command parser and submitter
    const parseStep1Command = new tasks.LambdaInvoke(this, 'ParseStep1Command', {
      lambdaFunction: props.commandParserFunction,
      inputPath: '$',
      resultPath: '$.parsedCommand'
    });

    // Step 2 command parser and submitter
    const parseStep2Command = new tasks.LambdaInvoke(this, 'ParseStep2Command', {
      lambdaFunction: props.commandParserFunction,
      inputPath: '$',
      resultPath: '$.parsedCommand'
    });

    // Set the Step 1 processor chain
    const submitStep1Job = new tasks.BatchSubmitJob(this, 'SubmitStep1Job', {
      jobName: sfn.JsonPath.stringAt('$.jobId'),
      jobQueueArn: props.batchJobQueue.attrJobQueueArn,
      jobDefinitionArn: props.batchJobDefinition.ref,
      containerOverrides: {
        command: sfn.JsonPath.listAt('$.parsedCommand.Payload.jobSubmission.ContainerOverrides.Command')
      },
      integrationPattern: sfn.IntegrationPattern.RUN_JOB
    });

    // Step 2 command parser and submitter
    const submitStep2Job = new tasks.BatchSubmitJob(this, 'SubmitStep2Job', {
      jobName: sfn.JsonPath.stringAt('$.jobId'),
      jobQueueArn: props.batchJobQueue.attrJobQueueArn,
      jobDefinitionArn: props.batchJobDefinition.ref,
      containerOverrides: {
        command: sfn.JsonPath.listAt('$.parsedCommand.Payload.jobSubmission.ContainerOverrides.Command')
      },
      integrationPattern: sfn.IntegrationPattern.RUN_JOB
    });

    // Chain the command parsers with job submitters
    const step1ProcessorChain = parseStep1Command.next(submitStep1Job);
    const step2ProcessorChain = parseStep2Command.next(submitStep2Job);
    
    // Create error handler task for batch job failures
    const handleJobErrors = new tasks.LambdaInvoke(this, 'HandleJobErrors', {
      lambdaFunction: props.errorHandlerFunction,
      inputPath: '$',
      resultPath: '$.errorHandlingResult',
      payload: sfn.TaskInput.fromObject({
        "workflowId.$": "$.workflowId",
        "failedJobs.$": "$.error.Cause[*].ErrorDetails"
      })
    });

    // Create workflow failure state
    const workflowFailed = new sfn.Fail(this, 'WorkflowFailed', {
      cause: 'Job execution failed',
      error: 'BatchJobError'
    });

    // Transition to workflow failure after handling errors
    handleJobErrors.next(workflowFailed);
    
    // Submit step 1 jobs - no need for Wait state as submitJob.sync handles it
    const submitStep1Jobs = new sfn.Map(this, 'SubmitStep1Jobs', {
      maxConcurrency: 1, // Step 1 is always a single job
      itemsPath: '$.JobCalculation.Payload.step1Jobs',
      itemSelector: {
        'jobId.$': '$$.Map.Item.Value.jobId',
        'Command.$': '$$.Map.Item.Value.Command',               // Job Calculator will provide this
        'stepNumber.$': '$$.Map.Item.Value.stepNumber',
        'workflowId.$': '$.workflowId'
      },
      resultPath: '$.Step1BatchJobs',
    });
    
    // Add error catching to the Map state
    submitStep1Jobs.addCatch(handleJobErrors, {
      errors: ['States.ALL'],
      resultPath: '$.error'
    });
    
    // Set the Step 1 processor chain
    submitStep1Jobs.itemProcessor(step1ProcessorChain);
    
    // Submit step 2 jobs directly after Step 1 - no need for waiting or checking
    const submitStep2Jobs = new sfn.Map(this, 'SubmitStep2Jobs', {
      maxConcurrency: 50,
      itemsPath: '$.JobCalculation.Payload.step2Jobs',
      itemSelector: {
        'jobId.$': '$$.Map.Item.Value.jobId',
        'Command.$': '$$.Map.Item.Value.Command',               // Job Calculator will provide this
        'stepNumber.$': '$$.Map.Item.Value.stepNumber',
        'workflowId.$': '$.workflowId'
      },
      resultPath: '$.Step2BatchJobs',
    });
    
    // Add error catching to the Map state
    submitStep2Jobs.addCatch(handleJobErrors, {
      errors: ['States.ALL'],
      resultPath: '$.error'
    });
    
    // Set the Step 2 processor chain
    submitStep2Jobs.itemProcessor(step2ProcessorChain);
    
    // Handle successful completion with Lambda
    const handleSuccess = new tasks.LambdaInvoke(this, 'HandleSuccess', {
      lambdaFunction: props.successHandlerFunction,
      inputPath: '$',
      resultPath: '$.successResult',
      payload: sfn.TaskInput.fromObject({
        "workflowId.$": "$.workflowId",
        "resultsBucketPath.$": "$.resultsBucketPath",
        "completionTime.$": "$$.State.EnteredTime"
      })
    });
    
    // Final success state
    const workflowSuccess = new sfn.Succeed(this, 'WorkflowSucceeded');

    // Workflow flow
    initializeWorkflow.next(calculateBatchJobs);
    calculateBatchJobs.next(submitStep1Jobs);
    submitStep1Jobs.next(submitStep2Jobs);
    submitStep2Jobs.next(handleSuccess);
    handleSuccess.next(workflowSuccess);

    return initializeWorkflow;
  }
} 