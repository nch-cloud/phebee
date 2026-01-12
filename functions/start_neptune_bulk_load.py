import json
import logging
import os
from phebee.utils.neptune import start_load

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """Start Neptune bulk load operations for TTL files organized by graph"""
    logger.info(event)
    
    try:
        run_id = event.get('run_id')
        ttl_prefix = event.get('ttl_prefix')  # s3://bucket/runs/run_id/neptune/
        load_type = event.get('load_type', 'all')  # 'projects', 'subjects', or 'all'
        
        if not run_id:
            raise ValueError("run_id is required")
        
        if not ttl_prefix:
            raise ValueError("ttl_prefix is required")
        
        # Get environment variables
        region = os.environ.get('AWS_REGION')
        
        load_results = []
        
        # Discover TTL directories from S3
        import boto3
        s3 = boto3.client('s3')
        bucket = ttl_prefix.split('/')[2]  # Extract bucket from s3://bucket/path/
        prefix = '/'.join(ttl_prefix.split('/')[3:])  # Extract prefix after bucket
        
        # Determine which TTL directories to process based on load_type
        ttl_directories = {}
        
        if load_type in ['projects', 'all']:
            # List objects to find project directories
            projects_prefix = f"{prefix}projects/"
            response = s3.list_objects_v2(Bucket=bucket, Prefix=projects_prefix, Delimiter='/')
            
            for common_prefix in response.get('CommonPrefixes', []):
                project_dir = common_prefix['Prefix']
                project_id = project_dir.split('/')[-2]  # Extract project ID from path
                ttl_directories[f'project_{project_id}'] = f"s3://{bucket}/{project_dir}"
        
        if load_type in ['subjects', 'all']:
            # Add subjects directory
            subjects_dir = f"{prefix}subjects/"
            ttl_directories['subjects'] = f"s3://{bucket}/{subjects_dir}"
        
        if not ttl_directories:
            logger.warning(f"No TTL directories found for load_type: {load_type}")
            return {
                'statusCode': 200,
                'body': {
                    'run_id': run_id,
                    'loads': [],
                    'total_loads': 0
                }
            }
        projects_prefix = f"{prefix}projects/"
        response = s3.list_objects_v2(Bucket=bucket, Prefix=projects_prefix, Delimiter='/')
        
        for common_prefix in response.get('CommonPrefixes', []):
            project_dir = common_prefix['Prefix']
            project_id = project_dir.split('/')[-2]  # Extract project ID from path
            ttl_directories[f'project_{project_id}'] = f"s3://{bucket}/{project_dir}"
        
        # Process each TTL directory
        for graph_name, source_dir in ttl_directories.items():
            
            # Check if directory actually contains TTL files
            bucket_name = source_dir.replace('s3://', '').split('/')[0]
            dir_prefix = '/'.join(source_dir.replace('s3://', '').split('/')[1:])
            
            # List TTL files in this directory
            ttl_response = s3.list_objects_v2(Bucket=bucket_name, Prefix=dir_prefix)
            ttl_files = [obj for obj in ttl_response.get('Contents', []) if obj['Key'].endswith('.ttl')]
            
            if not ttl_files:
                print(f"No TTL files found in {source_dir}, skipping load")
                continue
            
            # Determine named graph URI based on graph type
            if graph_name == 'subjects':
                named_graph_uri = "http://ods.nationwidechildrens.org/phebee/subjects"
            elif graph_name.startswith('project_'):
                project_id = graph_name[8:]  # Remove 'project_' prefix (8 characters)
                named_graph_uri = f"http://ods.nationwidechildrens.org/phebee/projects/{project_id}"
            else:
                logger.warning(f"Unknown graph type: {graph_name}")
                continue
            
            load_params = {
                "source": source_dir,
                "format": "turtle",
                "iamRoleArn": os.environ.get('NEPTUNE_LOAD_ROLE_ARN', ''),
                "region": region,
                "failOnError": "FALSE",
                "parallelism": "HIGH",
                "updateSingleCardinalityProperties": "FALSE",
                "queueRequest": "TRUE"
            }
            
            # Add namedGraphUri for both projects and subjects
            load_params["parserConfiguration"] = {
                "baseUri": "http://ods.nationwidechildrens.org/phebee",
                "namedGraphUri": named_graph_uri
            }
            
            logger.info(f"Starting Neptune bulk load for {graph_name}")
            logger.info(f"TTL source: {source_dir}")
            logger.info(f"Named graph URI: {named_graph_uri}")
            
            # Use existing Neptune utility to start load
            load_result = start_load(load_params)
            
            load_id = load_result['payload']['loadId']
            logger.info(f"Started Neptune bulk load with ID: {load_id} for {graph_name}")
            
            load_results.append({
                'name': graph_name,
                'load_id': load_id,
                'source': source_dir,
                'named_graph_uri': named_graph_uri,
                'status': 'LOAD_IN_PROGRESS'
            })
        
        return {
            'statusCode': 200,
            'body': {
                'run_id': run_id,
                'loads': load_results,
                'total_loads': len(load_results)
            }
        }
        
    except Exception as e:
        logger.exception("Failed to start Neptune bulk loads")
        return {
            'statusCode': 500,
            'body': {
                'error': str(e),
                'run_id': event.get('run_id')
            }
        }
