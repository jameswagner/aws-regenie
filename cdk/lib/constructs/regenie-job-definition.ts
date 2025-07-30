import * as cdk from 'aws-cdk-lib';
import * as batch from 'aws-cdk-lib/aws-batch';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as ecr from 'aws-cdk-lib/aws-ecr';
import * as fsx from 'aws-cdk-lib/aws-fsx';
import { Construct } from 'constructs';

export interface RegenieJobDefinitionProps {
  readonly regenieRepository: ecr.IRepository;
  readonly regenieVersion?: string;
  readonly jobRoleArn: string;
  readonly fileSystem: fsx.LustreFileSystem;
  readonly vcpus?: number;
  readonly memoryMiB?: number;
}

export class RegenieJobDefinition extends Construct {
  public readonly jobDefinition: batch.CfnJobDefinition;
  
  constructor(scope: Construct, id: string, props: RegenieJobDefinitionProps) {
    super(scope, id);
    
    const version = props.regenieVersion || 'latest';
    
    // Create the job definition
    this.jobDefinition = new batch.CfnJobDefinition(this, 'JobDefinition', {
      type: 'container',
      containerProperties: {
        image: `${props.regenieRepository.repositoryUri}:${version}`,
        jobRoleArn: props.jobRoleArn,
        resourceRequirements: [
          {
            type: 'VCPU',
            value: props.vcpus?.toString() || '4'
          },
          {
            type: 'MEMORY',
            value: props.memoryMiB?.toString() || '16384'
          }
        ],
        command: [
          "/bin/bash", 
          "-c", 
          [
            "cd /mnt/fsx",
            "echo 'Using pre-mounted FSx filesystem at /mnt/fsx'",
            "# Run the actual command passed by the job (we use eval to handle complex commands)",
            "eval \"$@\""
          ].join(" && ")
        ],
        logConfiguration: {
          logDriver: 'awslogs',
          options: {
            'awslogs-region': cdk.Stack.of(this).region,
            'awslogs-group': `/aws/batch/regenie-${version}`
          }
        },
        volumes: [
          {
            name: 'fsx-lustre',
            host: {
              sourcePath: '/mnt/fsx'
            }
          }
        ],
        mountPoints: [
          {
            containerPath: '/mnt/fsx',
            readOnly: false,
            sourceVolume: 'fsx-lustre'
          }
        ]
      },
      retryStrategy: {
        attempts: 2
      },
      propagateTags: true,
      timeout: {
        attemptDurationSeconds: 7200 // 2 hours
      }
    });
    
    // Add output for job definition
    new cdk.CfnOutput(this, 'JobDefinitionArn', {
      value: this.jobDefinition.ref,
      description: `Regenie Job Definition (${version}) ARN`
    });
  }
} 