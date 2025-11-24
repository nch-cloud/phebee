import json
import logging
import boto3
import requests
from urllib.parse import quote

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """Start Neptune bulk load operation"""
    logger.info(event)
    
    try:
        run_id = event.get('run_id')
        emr_job_id = event.get('ttl_files')  # This is actually the EMR job ID
        neptune_cluster = event.get('neptune_cluster')
        
        if not all([run_id, neptune_cluster]):
            raise ValueError("run_id and neptune_cluster are required")
        
        # Construct TTL file path based on run_id
        bucket_name = boto3.Session().region_name  # Get from environment or config
        ttl_source = f"s3://{bucket_name}/phebee/runs/{run_id}/neptune/"
        
        # Neptune bulk load API endpoint
        neptune_endpoint = f"https://{neptune_cluster}:8182"
        load_url = f"{neptune_endpoint}/loader"
        
        # Prepare bulk load request
        load_request = {
            "source": ttl_source,
            "format": "turtle",
            "iamRoleArn": f"arn:aws:iam::{boto3.client('sts').get_caller_identity()['Account']}:role/NeptuneLoadFromS3Role",
            "region": boto3.Session().region_name,
            "failOnError": "FALSE",
            "parallelism": "MEDIUM",
            "updateSingleCardinalityProperties": "FALSE",
            "queueRequest": "TRUE"
        }
        
        # Start bulk load
        logger.info(f"Starting Neptune bulk load from {ttl_source}")
        
        response = requests.post(
            load_url,
            json=load_request,
            headers={'Content-Type': 'application/json'},
            timeout=30
        )
        
        if response.status_code != 200:
            raise Exception(f"Neptune bulk load failed: {response.status_code} - {response.text}")
        
        load_result = response.json()
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
