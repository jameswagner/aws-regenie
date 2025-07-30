import * as cdk from 'aws-cdk-lib';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as python from '@aws-cdk/aws-lambda-python-alpha';
import { Construct } from 'constructs';

/**
 * Interface for Lambda function creation parameters
 */
export interface LambdaFunctionProps {
  scope: Construct;
  functionName: string;
  description: string;
  lambdaModule: string;  // e.g., 'workflow_init', 'job_calculator'
  timeout: cdk.Duration;
  memorySize: number;
  environment: { [key: string]: string };
  vpc: ec2.Vpc;
  role: iam.Role;
  ephemeralStorageSize?: cdk.Size;
}

/**
 * Utility class for creating Lambda functions with consistent configuration
 */
export class LambdaFactory {
  /**
   * Creates a Lambda function with standard configuration and best practices
   */
  public static createFunction(props: LambdaFunctionProps): lambda.Function {
    const {
      scope,
      functionName,
      description,
      lambdaModule,
      timeout,
      memorySize,
      environment,
      vpc,
      role,
      ephemeralStorageSize
    } = props;

    return new python.PythonFunction(scope, functionName, {
      entry: '../src/lambdas',  // Package the entire lambdas directory
      runtime: lambda.Runtime.PYTHON_3_12,
      index: `${lambdaModule}/lambda_function.py`,  // Point to specific module
      handler: 'handler',
      role: role,
      timeout: timeout,
      memorySize: memorySize,
      vpc: vpc,
      vpcSubnets: {
        subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS,
      },
      environment: environment,
      logRetention: logs.RetentionDays.ONE_YEAR,
      description: description,
      ...(ephemeralStorageSize && { ephemeralStorageSize })
    });
  }


} 