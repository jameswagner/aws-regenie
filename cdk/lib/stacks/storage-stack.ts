import * as cdk from 'aws-cdk-lib';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as fsx from 'aws-cdk-lib/aws-fsx';
import * as sns from 'aws-cdk-lib/aws-sns';
import * as events from 'aws-cdk-lib/aws-events';
import * as targets from 'aws-cdk-lib/aws-events-targets';
import * as iam from 'aws-cdk-lib/aws-iam';
import { Construct } from 'constructs';

interface StorageStackProps extends cdk.StackProps {
  vpc: ec2.Vpc;
  dataPrefix?: string; // For FSx/S3 data association prefix
}

export class StorageStack extends cdk.Stack {
  public readonly dataBucket: s3.Bucket;
  public readonly resultsBucket: s3.Bucket;
  public readonly fileSystem: fsx.LustreFileSystem;
  public readonly dataPrefix: string;
  public readonly manifestNotificationTopic: sns.Topic;
  public readonly fsxInputMountPath: string;
  public readonly fsxOutputMountPath: string;
  
  constructor(scope: Construct, id: string, props: StorageStackProps) {
    super(scope, id, props);
    
    // Use provided dataPrefix or default to "genomics"
    this.dataPrefix = props.dataPrefix || 'genomics';
    
    // Define mount paths
    this.fsxInputMountPath = '/mnt/fsx/input';
    this.fsxOutputMountPath = '/mnt/fsx/output';
    
    // Create S3 bucket for genomic data
    this.dataBucket = new s3.Bucket(this, 'GenomicDataBucket', {
      versioned: true,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      encryption: s3.BucketEncryption.S3_MANAGED,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      autoDeleteObjects: true,
      lifecycleRules: [
        {
          id: 'Infrequent-access-after-30-days',
          enabled: true,
          transitions: [
            {
              storageClass: s3.StorageClass.INFREQUENT_ACCESS,
              transitionAfter: cdk.Duration.days(30),
            },
          ],
        },
      ],
    });
    
    // Create S3 bucket for analysis results
    this.resultsBucket = new s3.Bucket(this, 'ResultsBucket', {
      versioned: true,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      encryption: s3.BucketEncryption.S3_MANAGED,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      autoDeleteObjects: true,
      lifecycleRules: [
        {
          id: 'Infrequent-after-30-days',
          enabled: true,
          transitions: [
            {
              storageClass: s3.StorageClass.INFREQUENT_ACCESS,
              transitionAfter: cdk.Duration.days(30),
            },
          ],
        },
      ],
    });
    
    // Add bucket policy for FSx access to data bucket
    this.dataBucket.addToResourcePolicy(new iam.PolicyStatement({
      actions: [
        's3:Get*',
        's3:List*',
        's3:PutObject',
        's3:AbortMultipartUpload',
        's3:DeleteObject',
        's3:PutBucketNotification'
      ],
      resources: [
        this.dataBucket.bucketArn,
        this.dataBucket.bucketArn + "/*",
        this.dataBucket.bucketArn + '/' + this.dataPrefix,
        `${this.dataBucket.bucketArn}/${this.dataPrefix}`,
        `${this.dataBucket.bucketArn}/${this.dataPrefix}/*`
      ],
      principals: [new iam.ServicePrincipal('fsx.amazonaws.com')]
    }));
    
    // Add bucket policy for FSx access to results bucket
    this.resultsBucket.addToResourcePolicy(new iam.PolicyStatement({
      actions: [
        's3:Get*',
        's3:List*',
        's3:PutObject',
        's3:AbortMultipartUpload',
        's3:DeleteObject',
        's3:PutBucketNotification'
      ],
      resources: [
        this.resultsBucket.bucketArn,
        this.resultsBucket.bucketArn + "/*"
      ],
      principals: [new iam.ServicePrincipal('fsx.amazonaws.com')]
    }));
    
    // Create an SNS Topic for S3 manifest file upload notifications
    this.manifestNotificationTopic = new sns.Topic(this, 'ManifestNotificationTopic', {
      displayName: 'GWAS Manifest File Notifications'
    });
    
    // Enable EventBridge on the S3 bucket (doesn't conflict with FSx DRA!)
    this.dataBucket.enableEventBridgeNotification();

    
    
    // Create EventBridge rule for manifest files (.json) - targets existing SNS topic
    const manifestRule = new events.Rule(this, 'ManifestEventBridgeRule', {
      description: 'Trigger on .json manifest file uploads to data bucket',
      eventPattern: {
        source: ['aws.s3'],
        detailType: ['Object Created'],
        detail: {
          bucket: { 
            name: [this.dataBucket.bucketName] 
          },
          object: { 
            key: [{ suffix: '.json' }] 
          }
        }
      }
    });
    
    // Target the existing SNS topic - keeps your SNS -> SQS -> Lambda flow unchanged
    manifestRule.addTarget(new targets.SnsTopic(this.manifestNotificationTopic));
    
    // Create FSx for Lustre file system with separate mount points for input and output
    const lustre_configuration = {
      deploymentType: fsx.LustreDeploymentType.SCRATCH_2
      // DRAs will handle import/export policies for specific mount points
    };
    
    this.fileSystem = new fsx.LustreFileSystem(this, 'FsxLustreFileSystem', {
      vpc: props.vpc,
      vpcSubnet: props.vpc.privateSubnets[0],
      storageCapacityGiB: 1200,
      lustreConfiguration: lustre_configuration,
      fileSystemTypeVersion: fsx.FileSystemTypeVersion.V_2_12,
      removalPolicy: cdk.RemovalPolicy.DESTROY
    });
    
    // Create Data Repository Association for input data (data bucket -> /input mount)
    const inputDra = new fsx.CfnDataRepositoryAssociation(this, 'InputDataRepositoryAssociation', {
      fileSystemId: this.fileSystem.fileSystemId,
      dataRepositoryPath: this.dataBucket.s3UrlForObject(),
      fileSystemPath: '/input',
      importedFileChunkSize: 1024,
      s3: {
        autoExportPolicy: {
          events: ['NEW', 'CHANGED', 'DELETED']
        },
        autoImportPolicy: {
          events: ['NEW', 'CHANGED', 'DELETED']
        }
      }
    });
    
    // Create Data Repository Association for output results (results bucket -> /output mount)  
    const outputDra = new fsx.CfnDataRepositoryAssociation(this, 'OutputDataRepositoryAssociation', {
      fileSystemId: this.fileSystem.fileSystemId,
      dataRepositoryPath: this.resultsBucket.s3UrlForObject(),
      fileSystemPath: '/output',
      importedFileChunkSize: 1024,
      s3: {
        autoExportPolicy: {
          events: ['NEW', 'CHANGED', 'DELETED']
        },
        autoImportPolicy: {
          events: ['NEW', 'CHANGED', 'DELETED']
        }
      }
    });
    
    // Add dependencies to ensure DRAs are created after the filesystem and buckets
    inputDra.node.addDependency(this.fileSystem);
    outputDra.node.addDependency(this.fileSystem);
    inputDra.node.addDependency(this.dataBucket);
    outputDra.node.addDependency(this.resultsBucket);
    
    // =========================================================================
    // CloudFormation Outputs - For external access to resource information
    // =========================================================================
    
    // Output bucket names for external scripts and tools
    new cdk.CfnOutput(this, 'GenomicDataBucketName', {
      value: this.dataBucket.bucketName,
      description: 'Name of the S3 bucket for genomic data storage',
      exportName: `${this.stackName}-GenomicDataBucketName`
    });
    
    new cdk.CfnOutput(this, 'DataBucketName', {
      value: this.dataBucket.bucketName,
      description: 'Name of the S3 bucket for genomic data storage (alternative name)',
      exportName: `${this.stackName}-DataBucketName`
    });
    
    new cdk.CfnOutput(this, 'ResultsBucketName', {
      value: this.resultsBucket.bucketName,
      description: 'Name of the S3 bucket for analysis results storage',
      exportName: `${this.stackName}-ResultsBucketName`
    });
    
    new cdk.CfnOutput(this, 'DataPrefix', {
      value: this.dataPrefix,
      description: 'Data prefix used for FSx/S3 integration',
      exportName: `${this.stackName}-DataPrefix`
    });
    
    new cdk.CfnOutput(this, 'FSxFileSystemId', {
      value: this.fileSystem.fileSystemId,
      description: 'FSx for Lustre File System ID',
      exportName: `${this.stackName}-FSxFileSystemId`
    });
    
    new cdk.CfnOutput(this, 'FSxMountName', {
      value: this.fileSystem.mountName,
      description: 'FSx for Lustre Mount Name',
      exportName: `${this.stackName}-FSxMountName`
    });
    
    new cdk.CfnOutput(this, 'FSxInputMountPath', {
      value: this.fsxInputMountPath,
      description: 'FSx input mount path for containers',
      exportName: `${this.stackName}-FSxInputMountPath`
    });
    
    new cdk.CfnOutput(this, 'FSxOutputMountPath', {
      value: this.fsxOutputMountPath,
      description: 'FSx output mount path for containers',
      exportName: `${this.stackName}-FSxOutputMountPath`
    });
    
    new cdk.CfnOutput(this, 'ManifestNotificationTopicArn', {
      value: this.manifestNotificationTopic.topicArn,
      description: 'SNS Topic ARN for manifest file notifications',
      exportName: `${this.stackName}-ManifestNotificationTopicArn`
    });
    
  }
}