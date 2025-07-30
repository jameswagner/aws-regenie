import * as cdk from 'aws-cdk-lib';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as batch from 'aws-cdk-lib/aws-batch';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as ecr from 'aws-cdk-lib/aws-ecr';
import * as fsx from 'aws-cdk-lib/aws-fsx';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as s3 from 'aws-cdk-lib/aws-s3';
import { Construct } from 'constructs';
import { RegenieJobDefinition } from '../constructs/regenie-job-definition';

/**
 * Props interface for the ComputeStack
 */
interface ComputeStackProps extends cdk.StackProps {
  vpc: ec2.Vpc;
  fileSystem: fsx.LustreFileSystem;
  batchSecurityGroup: ec2.SecurityGroup;
  dataBucket: s3.Bucket;
  jobVcpus?: number;
  jobMemoryMiB?: number;
}

/**
 * Compute Stack for the GWAS workflow application.
 * 
 * Provides the computational resources required for genomic analysis:
 * - ECR repository for the regenie container image
 * - AWS Batch compute environment and job queue for scalable processing
 * - IAM roles for Batch service, EC2 instances, and jobs
 * - Job definitions for regenie workflow steps
 */
export class ComputeStack extends cdk.Stack {
  // Public properties exposed to other stacks
  public readonly batchJobQueue: batch.CfnJobQueue;
  public readonly batchJobDefinition: batch.CfnJobDefinition;
  public readonly regenieRepo: ecr.Repository;
  
  constructor(scope: Construct, id: string, props: ComputeStackProps) {
    super(scope, id, props);

    // =========================================================================
    // ECR Repository - Container Image Storage
    // =========================================================================
    
    // Create ECR repository for regenie container images
    this.regenieRepo = new ecr.Repository(this, 'RegenieRepository', {
      repositoryName: 'gwas-regenie-v1',
      imageScanOnPush: true, // Security best practice to scan for vulnerabilities
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      lifecycleRules: [
        {
          maxImageCount: 5,
          description: 'Keep only the 5 most recent images',
        },
      ],
    });

    // =========================================================================
    // IAM Roles - Service, Instance, and Job Roles
    // =========================================================================
    
    // Create IAM role for Batch service to manage AWS resources
    const batchServiceRole = new iam.Role(this, 'BatchServiceRole', {
      assumedBy: new iam.ServicePrincipal('batch.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSBatchServiceRole'),
      ],
    });

    // Create IAM role for EC2 instances in the Batch compute environment
    // This role allows instances to register with ECS and run containers
    const ec2InstanceRole = new iam.Role(this, 'BatchEC2Role', {
      assumedBy: new iam.ServicePrincipal('ec2.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AmazonECSTaskExecutionRolePolicy'),
      ],
    });

    // Add ECS container instance permissions
    // These are required for EC2 instances to communicate with ECS service
    ec2InstanceRole.addToPolicy(new iam.PolicyStatement({
      actions: [
        'ecs:RegisterContainerInstance',
        'ecs:DeregisterContainerInstance',
        'ecs:DiscoverPollEndpoint',
        'ecs:Submit*',
        'ecs:Poll',
        'ecs:StartTelemetrySession'
      ],
      resources: ['*'],
    }));

    // Add CloudWatch Logs permissions to EC2 instance role
    // Allows the instances to create and write logs
    ec2InstanceRole.addToPolicy(new iam.PolicyStatement({
      actions: [
        'logs:CreateLogGroup',
        'logs:CreateLogStream',
        'logs:PutLogEvents',
        'logs:DescribeLogStreams'
      ],
      resources: [
        `arn:aws:logs:${this.region}:${this.account}:log-group:/aws/batch/*`,
        `arn:aws:logs:${this.region}:${this.account}:log-group:/aws/batch/regenie-*:*`
      ]
    }));

    // Create IAM role for Batch jobs (container runtime role)
    // This role defines permissions for the containers running inside the Batch jobs
    const batchJobRole = new iam.Role(this, 'BatchJobRole', {
      assumedBy: new iam.ServicePrincipal('ecs-tasks.amazonaws.com'),
    });

    // Add S3 permissions to batch job role - scoped to genomic data bucket only
    batchJobRole.addToPolicy(new iam.PolicyStatement({
      actions: [
        's3:GetObject',
        's3:PutObject',
        's3:DeleteObject',
        's3:GetObjectVersion',
        's3:DeleteObjectVersion',
        's3:RestoreObject',
        's3:ListBucket'
      ],
      resources: [
        props.dataBucket.bucketArn,
        `${props.dataBucket.bucketArn}/*`
      ]
    }));

    // Add CloudWatch Logs permissions to batch job role
    batchJobRole.addToPolicy(new iam.PolicyStatement({
      actions: [
        'logs:CreateLogGroup',
        'logs:CreateLogStream',
        'logs:PutLogEvents'
      ],
      resources: [
        `arn:aws:logs:${this.region}:${this.account}:log-group:/aws/batch/*`,
        `arn:aws:logs:${this.region}:${this.account}:log-group:/aws/batch/regenie-*:*`
      ]
    }));

    // Create an IAM instance profile for the EC2 instances
    // This is required by EC2 to assume the instance role
    const instanceProfile = new iam.CfnInstanceProfile(this, 'BatchInstanceProfile', {
      roles: [ec2InstanceRole.roleName],
    });

    // =========================================================================
    // Security Groups and Launch Template
    // =========================================================================
    
    // Use security group passed from Network stack
    const batchSecurityGroup = props.batchSecurityGroup;

    // FSx for Lustre mount configuration
    // The mountName is part of the FSx filesystem DNS name and is used for mounting
    const mountName = props.fileSystem.mountName;
    
    // UserData script for EC2 instances to mount FSx for Lustre
    // This installs the Lustre client and mounts the filesystem to /mnt/fsx
    const fsxUserData = `MIME-Version: 1.0
Content-Type: multipart/mixed; boundary="==MYBOUNDARY=="

--==MYBOUNDARY==
Content-Type: text/cloud-config; charset="us-ascii"

runcmd:
  - echo "Starting Batch instance setup at $(date)" > /var/log/batch-startup.log
  - echo "Installing Lustre client..." >> /var/log/batch-startup.log
  
  # Update system first
  - yum update -y
  
  # Install compatible Lustre client (2.12 or newer for FSx 2.15)
  - amazon-linux-extras install -y lustre  # This gets 2.12, not 2.10
  
  # Verify Lustre client installation
  - rpm -q lustre-client >> /var/log/batch-startup.log
  - echo "Lustre client version installed:" >> /var/log/batch-startup.log
  
  # Load Lustre modules
  - modprobe lustre
  - lsmod | grep lustre >> /var/log/batch-startup.log
  
  # Create mount points
  - mkdir -p /mnt/fsx
  - echo "Created mount point at /mnt/fsx" >> /var/log/batch-startup.log
  
  # Wait a moment for system to stabilize
  - sleep 10
  
  # Mount FSx with correct mount name (use actual mount name, not variable)
  - echo "Mounting FSx file system with mount name: ${mountName}" >> /var/log/batch-startup.log
  - mount -t lustre -o relatime,flock ${props.fileSystem.dnsName}@tcp:/${mountName} /mnt/fsx || (
      echo "FSx mount failed - debugging..." >> /var/log/fsx-mount-failure.log && 
      echo "Mount command: mount -t lustre -o relatime,flock ${props.fileSystem.dnsName}@tcp:/${mountName} /mnt/fsx" >> /var/log/fsx-mount-failure.log &&
      dmesg | tail -20 >> /var/log/fsx-mount-failure.log && 
      rpm -q lustre-client >> /var/log/fsx-mount-failure.log &&
      lsmod | grep lustre >> /var/log/fsx-mount-failure.log &&
      nslookup ${props.fileSystem.dnsName} >> /var/log/fsx-mount-failure.log &&
      exit 1
    )
  
  # Verify mount was successful
  - df -h | grep fsx >> /var/log/batch-startup.log
  - echo "FSx mounted successfully at /mnt/fsx" >> /var/log/batch-startup.log
  
  # Create subdirectories after successful mount
  - mkdir -p /mnt/fsx/input
  - mkdir -p /mnt/fsx/output
  - echo "Created input/output subdirectories" >> /var/log/batch-startup.log
  
  # List contents to verify
  - ls -la /mnt/fsx >> /var/log/batch-startup.log
  
  # Add to fstab for persistence (with _netdev for network dependency)
  - echo "${props.fileSystem.dnsName}@tcp:/${mountName} /mnt/fsx lustre relatime,flock,_netdev,x-systemd.automount 0 0" >> /etc/fstab
  - echo "Added FSx mount to fstab" >> /var/log/batch-startup.log
  
  # Test write permissions
  - touch /mnt/fsx/test-write-$(date +%s) && echo "Write test successful" >> /var/log/batch-startup.log || echo "Write test failed" >> /var/log/batch-startup.log
  
  - echo "Batch instance setup completed successfully at $(date)" >> /var/log/batch-startup.log

--==MYBOUNDARY==--`;

    // Create a launch template that contains the UserData script for mounting FSx
    const batchLaunchTemplate = new ec2.LaunchTemplate(this, 'BatchLaunchTemplate', {
      userData: ec2.UserData.custom(fsxUserData)
    });

    // =========================================================================
    // AWS Batch Compute Environment and Job Queue
    // =========================================================================
    
    // Create compute environment for AWS Batch
    // This defines the EC2 instances that will run the batch jobs
    const computeEnvironment = new batch.CfnComputeEnvironment(this, 'GwasComputeEnvironment', {
      type: 'MANAGED', // AWS Batch manages the compute resources
      computeResources: {
        type: 'SPOT', // Use spot instances for cost optimization
        maxvCpus: 1000,
        minvCpus: 1,  
        desiredvCpus: 1,
        instanceTypes: [
          'c6i.large', 'c6i.xlarge', 'c6i.2xlarge', 'c6i.4xlarge', 'c6i.8xlarge',
          'r6i.large', 'r6i.xlarge', 'r6i.2xlarge', 'r6i.4xlarge',
          'm6i.large', 'm6i.xlarge', 'm6i.2xlarge', 'm6i.4xlarge'
        ], 
        subnets: props.vpc.privateSubnets.map(subnet => subnet.subnetId), // Deploy in private subnets
        securityGroupIds: [batchSecurityGroup.securityGroupId],
        instanceRole: instanceProfile.attrArn,
        allocationStrategy: 'SPOT_CAPACITY_OPTIMIZED',
        launchTemplate: {
          launchTemplateId: batchLaunchTemplate.launchTemplateId,
          version: '$Latest' 
        },
        spotIamFleetRole: new iam.Role(this, 'SpotFleetRole', {
          assumedBy: new iam.ServicePrincipal('spotfleet.amazonaws.com'),
          managedPolicies: [
            iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AmazonEC2SpotFleetTaggingRole'),
          ],
        }).roleArn
      },
      serviceRole: batchServiceRole.roleArn,
    });

    // Create job queue for AWS Batch
    // Jobs are submitted to this queue and then scheduled on the compute environment
    this.batchJobQueue = new batch.CfnJobQueue(this, 'GwasJobQueue', {
      priority: 1, // Priority of this queue (higher numbers have higher priority)
      computeEnvironmentOrder: [
        {
          order: 1,
          computeEnvironment: computeEnvironment.ref
        }
      ]
    });

    // =========================================================================
    // Regenie Job Definition and Logging
    // =========================================================================
    
    // Create CloudWatch log group for Regenie Batch jobs
    const regenieVersion = 'v3.0.1.gz';
    const regenieLogGroup = new logs.LogGroup(this, 'RegenieLogGroup', {
      logGroupName: `/aws/batch/regenie-${regenieVersion}`,
      retention: logs.RetentionDays.ONE_YEAR,
      removalPolicy: cdk.RemovalPolicy.DESTROY
    });

    // Create regenie job definition with volume mount
    // This defines the container configuration for running regenie
    const regenieJobDef = new RegenieJobDefinition(this, 'RegenieJobDefinition', {
      regenieRepository: this.regenieRepo,
      regenieVersion: regenieVersion,
      jobRoleArn: batchJobRole.roleArn,
      fileSystem: props.fileSystem,
      vcpus: props.jobVcpus,
      memoryMiB: props.jobMemoryMiB
    });

    // Expose job definition for other stacks
    this.batchJobDefinition = regenieJobDef.jobDefinition;

    // =========================================================================
    // CloudFormation Outputs - For external access to resource information
    // =========================================================================
    
    // Output ECR repository information for external scripts and tools
    new cdk.CfnOutput(this, 'RegenieRepositoryName', {
      value: this.regenieRepo.repositoryName,
      description: 'Name of the ECR repository for regenie container images',
      exportName: `${this.stackName}-RegenieRepositoryName`
    });
    
    new cdk.CfnOutput(this, 'RegenieRepositoryUri', {
      value: this.regenieRepo.repositoryUri,
      description: 'URI of the ECR repository for regenie container images',
      exportName: `${this.stackName}-RegenieRepositoryUri`
    });
    
    new cdk.CfnOutput(this, 'BatchJobQueueName', {
      value: this.batchJobQueue.ref,
      description: 'Name of the AWS Batch job queue',
      exportName: `${this.stackName}-BatchJobQueueName`
    });
    
    new cdk.CfnOutput(this, 'BatchJobDefinitionName', {
      value: this.batchJobDefinition.ref,
      description: 'Name of the AWS Batch job definition',
      exportName: `${this.stackName}-BatchJobDefinitionName`
    });

  }
} 