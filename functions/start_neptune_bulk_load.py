import json
import logging
import os
from phebee.utils.neptune import start_load

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """Start Neptune bulk load operation using existing Neptune utilities"""
    logger.info(event)
    
    try:
        run_id = event.get('run_id')
        ttl_files = event.get('ttl_files', [])
        
        if not run_id:
            raise ValueError("run_id is required")
        
        if not ttl_files:
            raise ValueError("ttl_files list is required")
        
        # Extract the S3 prefix from the first TTL file path
        # Convert s3://bucket/path/file.ttl to s3://bucket/path/
        first_file = ttl_files[0] if isinstance(ttl_files, list) else ttl_files
        if first_file.endswith('.ttl'):
            # Remove the filename to get the directory prefix
            ttl_source = '/'.join(first_file.split('/')[:-1]) + '/'
        else:
            ttl_source = first_file
        
        # Get environment variables (set by CloudFormation)
        region = os.environ.get('AWS_REGION')
        
        # Prepare bulk load parameters for Neptune utilities
        load_params = {
            "source": ttl_source,  # S3 prefix that contains TTL files
            "format": "turtle",
            "iamRoleArn": os.environ.get('NEPTUNE_LOAD_ROLE_ARN', ''),
            "region": region,
            "failOnError": "FALSE",
            "parallelism": "MEDIUM",
            "updateSingleCardinalityProperties": "FALSE",
            "queueRequest": "TRUE"
        }
        
        logger.info(f"Starting Neptune bulk load for run {run_id}")
        logger.info(f"TTL source prefix: {ttl_source}")
        
        # Use existing Neptune utility to start load
        load_result = start_load(load_params)
        
        load_id = load_result['payload']['loadId']
        logger.info(f"Started Neptune bulk load with ID: {load_id}")
        
        return {
            'statusCode': 200,
            'body': {
                'load_id': load_id,
                'run_id': run_id,
                'source': ttl_source,
                'status': 'LOAD_IN_PROGRESS'
            }
        }
        
    except Exception as e:
        logger.exception("Failed to start Neptune bulk load")
        return {
            'statusCode': 500,
            'body': {
                'error': str(e),
                'run_id': event.get('run_id')
            }
        }
