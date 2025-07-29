#!/usr/bin/env python3
import boto3
import os
import subprocess
import json
import time
import argparse
import base64
import uuid

def parse_args():
    parser = argparse.ArgumentParser(description='Upload regenie Docker image to ECR, example files to S3, and trigger workflow via manifest')
    parser.add_argument('--stack-prefix', default='Gwas', help='Prefix for CDK stack names (default: "Gwas")')
    parser.add_argument('--image-tag', default='v3.0.1.gz', help='Regenie image tag to use')
    parser.add_argument('--dataset-prefix', default='genomics', help='Prefix for S3 dataset (default: "genomics" to match FSx Lustre integration)')
    parser.add_argument('--example-dir', default='./regenie-example', help='Directory with example files')
    parser.add_argument('--ecr-repo', help='ECR repository name (if not specified, will be retrieved from CloudFormation)')
    parser.add_argument('--bucket', help='S3 bucket name (if not specified, will be retrieved from CloudFormation)')
    parser.add_argument('--trait-type', default='bt', choices=['qt', 'bt'], help='Trait type: quantitative (qt) or binary (bt)')
    parser.add_argument('--chr', help='Specify a single chromosome to test in step 2')
    parser.add_argument('--chrList', help='Comma separated list of chromosomes to test in step 2 (e.g., "1,2,3,X")')
    parser.add_argument('--list-resources', action='store_true', help='List detected resources and exit')
    return parser.parse_args()

def get_resource_from_cloudformation(cf_client, stack_name, output_key):
    """Get a resource value from CloudFormation stack outputs"""
    try:
        print(f"Looking for {output_key} in stack {stack_name}...")
        response = cf_client.describe_stacks(StackName=stack_name)
        print(f"Found stack {stack_name}, looking for output {output_key}...")
        
        if 'Outputs' not in response['Stacks'][0]:
            print(f"Stack {stack_name} has no outputs defined")
            return None
            
        for output in response['Stacks'][0]['Outputs']:
            print(f"  Found output: {output['OutputKey']}")
            if output['OutputKey'] == output_key:
                print(f"  Found value for {output_key}: {output['OutputValue']}")
                return output['OutputValue']
        
        print(f"Output {output_key} not found in stack {stack_name}")
        return None
    except cf_client.exceptions.ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', '')
        if error_code == 'ValidationError':
            print(f"Stack {stack_name} not found.")
        else:
            print(f"Error getting {output_key} from {stack_name}: {e}")
        return None
    except Exception as e:
        print(f"Error getting {output_key} from {stack_name}: {e}")
        return None

def get_resources(args):
    """Get all required resource information from CloudFormation or use provided values"""
    cf_client = boto3.client('cloudformation')
    resources = {}
    
    print("\n--- Searching for CloudFormation resources ---")
    
    # List all stacks to help with debugging
    try:
        print("Available CloudFormation stacks:")
        stacks_response = cf_client.list_stacks(
            StackStatusFilter=[
                'CREATE_COMPLETE', 'UPDATE_COMPLETE', 'UPDATE_ROLLBACK_COMPLETE'
            ]
        )
        for stack in stacks_response['StackSummaries']:
            print(f"  - {stack['StackName']}")
    except Exception as e:
        print(f"Error listing stacks: {e}")
    
    # Get S3 bucket name
    if args.bucket:
        print(f"Using provided bucket name: {args.bucket}")
        resources['bucket_name'] = args.bucket
    else:
        storage_stack_name = f"{args.stack_prefix}StorageStack"
        resources['bucket_name'] = get_resource_from_cloudformation(
            cf_client, 
            storage_stack_name, 
            "GenomicDataBucketName"
        )
        
        # Try alternative output key if the first one doesn't work
        if not resources['bucket_name']:
            print("Trying alternative output key 'DataBucketName'...")
            resources['bucket_name'] = get_resource_from_cloudformation(
                cf_client, 
                storage_stack_name, 
                "DataBucketName"
            )
            
        if not resources['bucket_name']:
            raise ValueError("Could not find S3 bucket name. Please provide it with --bucket")
    
    # Get data prefix from StorageStack if it was exported
    storage_stack_name = f"{args.stack_prefix}StorageStack"
    data_prefix = get_resource_from_cloudformation(
        cf_client,
        storage_stack_name,
        "DataPrefix"
    )
    
    # If we found a data prefix, use it instead of the default
    if data_prefix:
        print(f"Found DataPrefix from StorageStack: {data_prefix}")
        resources['data_prefix'] = data_prefix
    else:
        # Use the command line argument as fallback
        resources['data_prefix'] = args.dataset_prefix
        print(f"Using dataset prefix from command line: {resources['data_prefix']}")
    
    # Get ECR repository name
    if args.ecr_repo:
        print(f"Using provided ECR repository name: {args.ecr_repo}")
        resources['ecr_repo'] = args.ecr_repo
    else:
        compute_stack_name = f"{args.stack_prefix}ComputeStack"
        resources['ecr_repo'] = get_resource_from_cloudformation(
            cf_client, 
            compute_stack_name, 
            "RegenieRepositoryName"
        )
        
        # Try alternative output key if the first one doesn't work
        if not resources['ecr_repo']:
            print("Trying alternative output key 'RegenieRepositoryUri'...")
            repo_uri = get_resource_from_cloudformation(
                cf_client, 
                compute_stack_name, 
                "RegenieRepositoryUri"
            )
            if repo_uri:
                # Extract the repository name from the URI
                resources['ecr_repo'] = repo_uri.split('/')[-1].split(':')[0]
                print(f"Extracted repository name from URI: {resources['ecr_repo']}")
        
        if not resources['ecr_repo']:
            resources['ecr_repo'] = "gwas-regenie-v1"  # Fallback to default
            print(f"Using default ECR repo name: {resources['ecr_repo']}")
    
    print("\n--- Resource detection complete ---")
    print("Retrieved resources:")
    print(f"  Bucket: {resources['bucket_name']}")
    print(f"  Data Prefix: {resources['data_prefix']}")
    print(f"  ECR Repository: {resources['ecr_repo']}")
    
    return resources

def check_image_exists_in_ecr(ecr_client, repo_name, image_tag):
    """Check if the image already exists in ECR"""
    try:
        response = ecr_client.list_images(repositoryName=repo_name)
        for image in response['imageIds']:
            if image.get('imageTag') == image_tag:
                print(f"Image {image_tag} already exists in ECR repository {repo_name}")
                return True
        return False
    except ecr_client.exceptions.RepositoryNotFoundException:
        print(f"Repository {repo_name} not found. It will be created by the CDK stack.")
        return False
    except Exception as e:
        print(f"Error checking ECR repository: {e}")
        return False

def upload_image_to_ecr(ecr_client, repo_name, image_tag):
    """Upload the regenie Docker image to ECR"""
    try:
        # Get account ID for ECR URI
        sts_client = boto3.client('sts')
        account_id = sts_client.get_caller_identity()["Account"]
        region = boto3.session.Session().region_name
        
        # Use the AWS CLI to handle ECR login (much simpler approach)
        ecr_uri_base = f"{account_id}.dkr.ecr.{region}.amazonaws.com"
        print(f"Logging in to ECR at {ecr_uri_base}...")
        login_cmd = f"aws ecr get-login-password --region {region} | docker login --username AWS --password-stdin {ecr_uri_base}"
        subprocess.run(login_cmd, shell=True, check=True)
        
        # Pull the regenie image
        docker_image = f"ghcr.io/rgcgithub/regenie/regenie:{image_tag}"
        print(f"Pulling image {docker_image}...")
        subprocess.run(f"docker pull {docker_image}", shell=True, check=True)
        
        # Tag the image for ECR
        ecr_uri = f"{ecr_uri_base}/{repo_name}:{image_tag}"
        print(f"Tagging image for ECR as {ecr_uri}...")
        subprocess.run(f"docker tag {docker_image} {ecr_uri}", shell=True, check=True)
        
        # Push to ECR
        print(f"Pushing image to ECR...")
        subprocess.run(f"docker push {ecr_uri}", shell=True, check=True)
        
        print(f"Successfully uploaded image to ECR: {ecr_uri}")
        return ecr_uri
    except Exception as e:
        print(f"Error uploading image to ECR: {e}")
        raise

def upload_example_files_to_s3(s3_client, bucket_name, dataset_prefix, example_dir):
    """Upload example files to S3"""
    try:
        if not os.path.exists(example_dir):
            print(f"Creating example directory {example_dir}...")
            os.makedirs(example_dir)
            
            # Download example files if they don't exist
            example_files = [
                "example.bed", "example.bim", "example.fam", 
                "phenotype_bin.txt", "covariates.txt"
            ]
            
            for file in example_files:
                url = f"https://raw.githubusercontent.com/rgcgithub/regenie/master/example/{file}"
                print(f"Downloading {url}...")
                subprocess.run(f'curl -o "{example_dir}/{file}" {url}', shell=True, check=True)
        
        # Generate a unique analysis directory name
        analysis_dir = f"analysis-{int(time.time())}-{str(uuid.uuid4())[:8]}"
        prefix = f"{dataset_prefix}/{analysis_dir}"
        print(f"Using S3 prefix: {prefix}")
        
        # Check if files already exist in S3
        try:
            s3_client.head_object(Bucket=bucket_name, Key=f"{prefix}/example.bed")
            print(f"Files already exist at s3://{bucket_name}/{prefix}/, skipping upload...")
            # Return the dataset path in S3 URI format (s3://bucket/prefix)
            dataset_path = f"s3://{bucket_name}/{prefix}"
            return dataset_path
        except s3_client.exceptions.ClientError:
            # Files don't exist, proceed with upload
            print(f"Files don't exist in S3, will upload...")
        
        # Upload all files in the example directory
        print(f"Uploading files to S3 bucket {bucket_name}/{prefix}/...")
        for filename in os.listdir(example_dir):
            file_path = os.path.join(example_dir, filename)
            if os.path.isfile(file_path):
                s3_key = f"{prefix}/{filename}"
                print(f"Uploading {filename} to S3...")
                s3_client.upload_file(file_path, bucket_name, s3_key)
        
        # Return the dataset path in S3 URI format (s3://bucket/prefix)
        dataset_path = f"s3://{bucket_name}/{prefix}"
        print(f"Successfully uploaded example files to S3: {dataset_path}")
        return dataset_path
    except Exception as e:
        print(f"Error uploading files to S3: {e}")
        raise

def create_and_upload_manifest(s3_client, bucket_name, s3_path, trait_type, chr_param=None, chr_list=None):
    """Create and upload a manifest.json file to trigger the workflow"""
    try:
        # Generate a unique experiment ID
        experiment_id = f"exp-{int(time.time())}-{str(uuid.uuid4())[:8]}"
        
        # Create the manifest content
        manifest = {
            "experimentId": experiment_id,
            "s3Path": s3_path,
            "inputData": {
                "format": "bed",
                "filePrefix": "example",
                "phenoFile": "phenotype_bin.txt",
                "phenoColumns": ["Y1"],
                "covarFile": "covariates.txt",
                "covarColumns": ["V1", "V2", "V3"],
                "catCovarColumns": []
            },
            "analysisParams": {
                "traitType": trait_type,
                "blockSize": 1000,
                "minMAC": 5,
                "threads": 8,
                "cv": 5,
                "lowmem": True
            },
            "outputParams": {
                "outPrefix": "results",
                "outputS3Path": f"{s3_path}/results/",
                "gz": True
            }
        }
        
        # Add chromosome parameters if provided
        if chr_param:
            manifest["analysisParams"]["chr"] = chr_param
        elif chr_list:
            manifest["analysisParams"]["chrList"] = chr_list.split(',')
        
        # Convert to JSON string
        manifest_json = json.dumps(manifest, indent=2)
        
        # Define the path for the manifest file
        s3_bucket = bucket_name
        # Extract the key prefix from the s3_path (s3://bucket/prefix)
        s3_prefix = s3_path.replace(f"s3://{s3_bucket}/", "")
        manifest_key = f"{s3_prefix}/manifest.json"
        
        # Upload the manifest file to S3
        print(f"Uploading manifest.json to s3://{s3_bucket}/{manifest_key}...")
        s3_client.put_object(
            Bucket=s3_bucket,
            Key=manifest_key,
            Body=manifest_json,
            ContentType='application/json'
        )
        
        manifest_url = f"s3://{s3_bucket}/{manifest_key}"
        print(f"Successfully uploaded manifest to {manifest_url}")
        
        return {
            "manifestUrl": manifest_url,
            "experimentId": experiment_id
        }
    except Exception as e:
        print(f"Error creating and uploading manifest: {e}")
        raise

def main():
    args = parse_args()
    
    # Configure AWS clients
    ecr_client = boto3.client('ecr')
    s3_client = boto3.client('s3')
    
    # Check if both chr and chrList are provided
    if args.chr and args.chrList:
        print("ERROR: Cannot specify both --chr and --chrList. Please use only one of these options.")
        return 1
    
    # Get resources from CloudFormation or from args
    resources = get_resources(args)
    
    # If --list-resources is specified, just display resources and exit
    if args.list_resources:
        print("\nResources detected. You can use these values in your command:")
        print(f"  --bucket {resources['bucket_name']} \\")
        print(f"  --ecr-repo {resources['ecr_repo']}")
        return
    
    # 1. Upload the regenie Docker image to ECR if not exists
    if not check_image_exists_in_ecr(ecr_client, resources['ecr_repo'], args.image_tag):
        upload_image_to_ecr(ecr_client, resources['ecr_repo'], args.image_tag)
    
    # 2. Upload example files to S3
    s3_path = upload_example_files_to_s3(s3_client, resources['bucket_name'], resources['data_prefix'], args.example_dir)
    
    # 3. Create and upload manifest.json to trigger the workflow
    result = create_and_upload_manifest(
        s3_client,
        resources['bucket_name'],
        s3_path,
        args.trait_type,
        args.chr,
        args.chrList
    )
    
    print(f"\nWorkflow trigger successful!")
    print(f"Manifest file uploaded to: {result['manifestUrl']}")
    print(f"Experiment ID: {result['experimentId']}")
    print(f"The system will automatically detect the manifest file and start the workflow.")
    print(f"You can monitor the workflow executions at: https://console.aws.amazon.com/states/home?region={boto3.session.Session().region_name}#/executions")
    print("\nNote: It may take up to a minute for the trigger system to detect the manifest and start the workflow.")

if __name__ == "__main__":
    main() 