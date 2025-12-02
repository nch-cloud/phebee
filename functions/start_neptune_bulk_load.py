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
        nq_files = event.get('nq_files', [])
        
        if not run_id:
            raise ValueError("run_id is required")
        
        if not nq_files:
            raise ValueError("nq_files list is required")
        
        # Extract the S3 prefix from the first N-Quads file path
        # Convert s3://bucket/path/file.nq to s3://bucket/path/
        first_file = nq_files[0] if isinstance(nq_files, list) else nq_files
        if first_file.endswith('.nq'):
            # Remove the filename to get the directory prefix
            nq_source = '/'.join(first_file.split('/')[:-1]) + '/'
        else:
            nq_source = first_file
        
        # Get environment variables (set by CloudFormation)
        region = os.environ.get('AWS_REGION')
        
        # Prepare bulk load parameters for Neptune utilities
        load_params = {
            "source": nq_source,  # S3 prefix that contains N-Quads files
            "format": "nquads",
            "iamRoleArn": os.environ.get('NEPTUNE_LOAD_ROLE_ARN', ''),
            "region": region,
            "failOnError": "FALSE",
            "parallelism": "HIGH",
            "updateSingleCardinalityProperties": "FALSE",
            "queueRequest": "TRUE"
        }
        
        logger.info(f"Starting Neptune bulk load for run {run_id}")
        logger.info(f"N-Quads source prefix: {nq_source}")
        
        # Use existing Neptune utility to start load
        load_result = start_load(load_params)
        
        load_id = load_result['payload']['loadId']
        logger.info(f"Started Neptune bulk load with ID: {load_id}")
        
        return {
            'statusCode': 200,
            'body': {
                'load_id': load_id,
                'run_id': run_id,
                'source': nq_source,
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
