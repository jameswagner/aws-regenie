import * as cdk from 'aws-cdk-lib';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import { Construct } from 'constructs';

/**
 * Interface for GSI configuration
 */
export interface GSIConfig {
  indexName: string;
  partitionKey: { name: string, type: dynamodb.AttributeType };
  sortKey?: { name: string, type: dynamodb.AttributeType };
  projectionType?: dynamodb.ProjectionType;
}

/**
 * Interface for table creation with optional GSIs
 */
export interface TableCreateConfig {
  partitionKey: { name: string, type: dynamodb.AttributeType };
  sortKey?: { name: string, type: dynamodb.AttributeType };
  globalSecondaryIndexes?: GSIConfig[];
}

/**
 * Factory class for creating standardized DynamoDB tables and GSIs
 */
export class DynamoDBTableFactory {
  private readonly scope: Construct;
  
  constructor(scope: Construct) {
    this.scope = scope;
  }
  
  /**
   * Creates a standard DynamoDB table with common configurations and optional GSIs
   */
  public createTable(
    id: string,
    tableConfig: TableCreateConfig,
    tableName?: string
  ): dynamodb.Table {
    const table = new dynamodb.Table(this.scope, id, {
      partitionKey: tableConfig.partitionKey,
      sortKey: tableConfig.sortKey,
      tableName: tableName,
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      pointInTimeRecoverySpecification: {
        pointInTimeRecoveryEnabled: true
      }
    });
    
    // Add all GSIs if provided
    if (tableConfig.globalSecondaryIndexes && tableConfig.globalSecondaryIndexes.length > 0) {
      tableConfig.globalSecondaryIndexes.forEach(gsiConfig => {
        this.addGSI(table, gsiConfig);
      });
    }
    
    return table;
  }
  
  /**
   * Adds a Global Secondary Index to a table
   */
  public addGSI(table: dynamodb.Table, gsiConfig: GSIConfig): void {
    table.addGlobalSecondaryIndex({
      indexName: gsiConfig.indexName,
      partitionKey: gsiConfig.partitionKey,
      sortKey: gsiConfig.sortKey,
      projectionType: gsiConfig.projectionType || dynamodb.ProjectionType.ALL
    });
  }
  

} 