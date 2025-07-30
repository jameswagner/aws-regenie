import * as cdk from 'aws-cdk-lib';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import { Construct } from 'constructs';

export class NetworkStack extends cdk.Stack {
  public readonly vpc: ec2.Vpc;
  public readonly batchSecurityGroup: ec2.SecurityGroup;

  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // =========================================================================
    // VPC Configuration
    // =========================================================================
    // Create VPC with public and private subnets
    this.vpc = new ec2.Vpc(this, 'GwasVpc', {
      maxAzs: 2,
      natGateways: 1,
      subnetConfiguration: [
        {
          name: 'public',
          subnetType: ec2.SubnetType.PUBLIC,
          cidrMask: 24,
        },
        {
          name: 'private',
          subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS,
          cidrMask: 24,
        },
        {
          name: 'isolated',
          subnetType: ec2.SubnetType.PRIVATE_ISOLATED,
          cidrMask: 24,
        }
      ]
    });

    // =========================================================================
    // Security Group Definitions
    // =========================================================================
    // Create security group for FSx for Lustre file system
    const fsxSecurityGroup = new ec2.SecurityGroup(this, 'FsxSecurityGroup', {
      vpc: this.vpc,
      description: 'Security group for FSx for Lustre file system',
      allowAllOutbound: true,
    });

    // Create security group for AWS Batch compute environment
    this.batchSecurityGroup = new ec2.SecurityGroup(this, 'BatchSecurityGroup', {
      vpc: this.vpc,
      description: 'Security group for AWS Batch compute environment',
      allowAllOutbound: true,
    });

    // =========================================================================
    // FSx-Batch Communication Rules
    // =========================================================================
    // FSx Lustre requires specific ports for client-server communication:
    // - Port 988: Main Lustre protocol communication
    // - Ports 1018-1023: Additional Lustre service ports

    // 1. Allow Batch instances to access FSx Lustre server
    fsxSecurityGroup.addIngressRule(
      this.batchSecurityGroup,
      ec2.Port.tcp(988),
      'Allow Batch instances to access FSx Lustre on port 988 (primary Lustre protocol)'
    );

    fsxSecurityGroup.addIngressRule(
      this.batchSecurityGroup,
      ec2.Port.tcpRange(1018, 1023),
      'Allow Batch instances to access FSx Lustre on ports 1018-1023 (Lustre services)'
    );

    // 2. Allow FSx servers to communicate back to Batch instances
    // This is needed for the bi-directional communication Lustre requires
    this.batchSecurityGroup.addIngressRule(
      fsxSecurityGroup,
      ec2.Port.tcp(988),
      'Allow FSx servers to communicate with Batch instances on port 988 (primary Lustre protocol)'
    );

    this.batchSecurityGroup.addIngressRule(
      fsxSecurityGroup,
      ec2.Port.tcpRange(1018, 1023),
      'Allow FSx servers to communicate with Batch instances on ports 1018-1023 (Lustre services)'
    );

  }
} 