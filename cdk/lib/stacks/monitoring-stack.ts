import * as cdk from 'aws-cdk-lib';
import * as cloudwatch from 'aws-cdk-lib/aws-cloudwatch';
import * as sns from 'aws-cdk-lib/aws-sns';
import * as subscriptions from 'aws-cdk-lib/aws-sns-subscriptions';
import * as sqs from 'aws-cdk-lib/aws-sqs';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as sfn from 'aws-cdk-lib/aws-stepfunctions';
import * as batch from 'aws-cdk-lib/aws-batch';
import * as cw_actions from 'aws-cdk-lib/aws-cloudwatch-actions';
import { Construct } from 'constructs';

interface MonitoringStackProps extends cdk.StackProps {
  manifestDeadLetterQueue: sqs.Queue;
  manifestQueue?: sqs.Queue;
  manifestProcessorFunction?: lambda.Function;
  manifestTriggerFunction?: lambda.Function;
  stateMachine?: sfn.StateMachine;
  batchJobQueue?: batch.CfnJobQueue;
  notificationEmail?: string; // Optional email for notifications
}

/**
 * Stack for CloudWatch alarms and monitoring dashboards
 * Focused on the three key components: Lambda, Step Functions, and Batch
 */
export class MonitoringStack extends cdk.Stack {
  public readonly alarmTopic: sns.Topic;
  
  constructor(scope: Construct, id: string, props: MonitoringStackProps) {
    super(scope, id, props);
    
    // Create an SNS topic for alarms
    this.alarmTopic = new sns.Topic(this, 'AlarmTopic', {
      displayName: 'GWAS Workflow Alarms'
    });
    
    // If an email is provided, add an email subscription to the alarm topic
    if (props.notificationEmail) {
      this.alarmTopic.addSubscription(
        new subscriptions.EmailSubscription(props.notificationEmail)
      );
    }
    
    // Create the key alarms
    
    // 1. LAMBDA ALARM: Manifest Processor Lambda Errors
    if (props.manifestProcessorFunction) {
      const lambdaErrorsAlarm = new cloudwatch.Alarm(this, 'LambdaErrorsAlarm', {
        alarmName: 'GWAS-LambdaErrors',
        alarmDescription: 'Alarm for Lambda function errors which will block workflow triggers',
        metric: props.manifestProcessorFunction.metricErrors({
          period: cdk.Duration.minutes(5),
          statistic: 'Sum',
        }),
        threshold: 3, // Alert after 3 errors
        evaluationPeriods: 1,
        comparisonOperator: cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
        treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
      });
      
      lambdaErrorsAlarm.addAlarmAction(new cw_actions.SnsAction(this.alarmTopic));
    }
    
    // New alarm: Manifest Trigger Lambda Errors
    if (props.manifestTriggerFunction) {
      const triggerLambdaErrorsAlarm = new cloudwatch.Alarm(this, 'TriggerLambdaErrorsAlarm', {
        alarmName: 'GWAS-TriggerLambdaErrors',
        alarmDescription: 'Alarm for Manifest Trigger Lambda function errors',
        metric: props.manifestTriggerFunction.metricErrors({
          period: cdk.Duration.minutes(5),
          statistic: 'Sum',
        }),
        threshold: 3, // Alert after 3 errors
        evaluationPeriods: 1,
        comparisonOperator: cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
        treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
      });
      
      triggerLambdaErrorsAlarm.addAlarmAction(new cw_actions.SnsAction(this.alarmTopic));
    }
    
    // 2. STEP FUNCTION ALARM: Workflow Execution Failures
    if (props.stateMachine) {
      const stateMachineFailedAlarm = new cloudwatch.Alarm(this, 'StateMachineFailedAlarm', {
        alarmName: 'GWAS-WorkflowFailed',
        alarmDescription: 'Alarm for Step Functions workflow failures',
        metric: props.stateMachine.metricFailed({
          period: cdk.Duration.minutes(5),
          statistic: 'Sum',
        }),
        threshold: 1, // Alert on any workflow failure
        evaluationPeriods: 1,
        comparisonOperator: cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
        treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
      });
      
      stateMachineFailedAlarm.addAlarmAction(new cw_actions.SnsAction(this.alarmTopic));
    }
    
    // 3. BATCH JOBS ALARM: Failed Jobs Metric
    // Create a custom metric for failed Batch jobs using metric math
    if (props.batchJobQueue) {
      // Get the job queue name from the job queue ARN
      const jobQueueName = props.batchJobQueue.jobQueueName || 'GWASJobQueue';
      
      // Create custom metrics for Batch job failures
      const batchFailedJobs = new cloudwatch.Metric({
        namespace: 'AWS/Batch',
        metricName: 'FailedJobCount',
        dimensionsMap: {
          JobQueue: jobQueueName,
        },
        period: cdk.Duration.minutes(5),
        statistic: 'Sum',
      });
      
      const batchFailedJobsAlarm = new cloudwatch.Alarm(this, 'BatchFailedJobsAlarm', {
        alarmName: 'GWAS-BatchJobsFailed',
        alarmDescription: 'Alarm for AWS Batch job failures in the GWAS workflow',
        metric: batchFailedJobs,
        threshold: 1, // Alert on any batch job failure
        evaluationPeriods: 1,
        comparisonOperator: cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
        treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
      });
      
      batchFailedJobsAlarm.addAlarmAction(new cw_actions.SnsAction(this.alarmTopic));
    }
    
    // Create a dashboard focused on these three key components
    const dashboard = new cloudwatch.Dashboard(this, 'GwasDashboard', {
      dashboardName: 'GWAS-Critical-Monitoring'
    });
    
    // Add focused widgets for the key components
    dashboard.addWidgets(
      // Title
      new cloudwatch.TextWidget({
        markdown: '# GWAS Critical Monitoring\nKey metrics for the main components',
        width: 24,
        height: 2,
      }),
      
      // Lambda Processing Widget (Component 1)
      ...(props.manifestProcessorFunction ? [
        new cloudwatch.GraphWidget({
          title: '1. Lambda Processing (Manifest Processor)',
          left: [
            props.manifestProcessorFunction.metricInvocations({
              period: cdk.Duration.minutes(5),
              statistic: 'Sum',
              label: 'Invocations',
            }),
            props.manifestProcessorFunction.metricErrors({
              period: cdk.Duration.minutes(5),
              statistic: 'Sum',
              label: 'Errors',
            }),
          ],
          width: 8,
        })
      ] : []),
      
      // Manifest Trigger Lambda Widget
      ...(props.manifestTriggerFunction ? [
        new cloudwatch.GraphWidget({
          title: '1b. Manifest Trigger Lambda',
          left: [
            props.manifestTriggerFunction.metricInvocations({
              period: cdk.Duration.minutes(5),
              statistic: 'Sum',
              label: 'Invocations',
            }),
            props.manifestTriggerFunction.metricErrors({
              period: cdk.Duration.minutes(5),
              statistic: 'Sum',
              label: 'Errors',
            }),
            props.manifestTriggerFunction.metricDuration({
              period: cdk.Duration.minutes(5),
              statistic: 'Average',
              label: 'Duration (avg)',
            }),
          ],
          width: 8,
        })
      ] : []),
      
      // Step Functions Widget (Component 2)
      ...(props.stateMachine ? [
        new cloudwatch.GraphWidget({
          title: '2. Workflow Executions (Step Functions)',
          left: [
            props.stateMachine.metricStarted({
              period: cdk.Duration.minutes(30),
              statistic: 'Sum',
              label: 'Started',
            }),
            props.stateMachine.metricSucceeded({
              period: cdk.Duration.minutes(30),
              statistic: 'Sum',
              label: 'Succeeded',
            }),
            props.stateMachine.metricFailed({
              period: cdk.Duration.minutes(30),
              statistic: 'Sum',
              label: 'Failed',
            }),
          ],
          width: 8,
        })
      ] : []),
      
      // Batch Jobs Widget (Component 3)
      ...(props.batchJobQueue ? [
        new cloudwatch.GraphWidget({
          title: '3. Batch Job Status',
          left: [
            new cloudwatch.Metric({
              namespace: 'AWS/Batch',
              metricName: 'SubmittedJobCount',
              dimensionsMap: {
                JobQueue: props.batchJobQueue.jobQueueName || 'GWASJobQueue',
              },
              period: cdk.Duration.minutes(5),
              statistic: 'Sum',
              label: 'Submitted Jobs',
            }),
            new cloudwatch.Metric({
              namespace: 'AWS/Batch',
              metricName: 'RunningJobCount',
              dimensionsMap: {
                JobQueue: props.batchJobQueue.jobQueueName || 'GWASJobQueue',
              },
              period: cdk.Duration.minutes(5),
              statistic: 'Sum',
              label: 'Running Jobs',
            }),
            new cloudwatch.Metric({
              namespace: 'AWS/Batch',
              metricName: 'FailedJobCount',
              dimensionsMap: {
                JobQueue: props.batchJobQueue.jobQueueName || 'GWASJobQueue',
              },
              period: cdk.Duration.minutes(5),
              statistic: 'Sum',
              label: 'Failed Jobs',
            }),
          ],
          width: 8,
        })
      ] : []),
      
      // Add SQS/DLQ information as a supporting metric
      new cloudwatch.GraphWidget({
        title: 'Supporting Metrics: SQS and DLQ',
        left: [
          props.manifestDeadLetterQueue.metricApproximateNumberOfMessagesVisible({
            period: cdk.Duration.minutes(5),
            statistic: 'Sum',
            label: 'DLQ Messages',
          }),
          ...(props.manifestQueue ? [
            props.manifestQueue.metricApproximateNumberOfMessagesVisible({
              period: cdk.Duration.minutes(5),
              statistic: 'Sum',
              label: 'Queue Messages',
            }),
            props.manifestQueue.metricApproximateAgeOfOldestMessage({
              period: cdk.Duration.minutes(5),
              statistic: 'Maximum',
              label: 'Max Message Age (sec)',
            })
          ] : [])
        ],
        width: 24,
      })
    );
  }
} 