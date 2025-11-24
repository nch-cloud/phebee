import json
import logging
import os
import requests

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """Check Neptune bulk load status"""
    logger.info(event)
    
    try:
        load_id = event.get('load_id')
        
        if not load_id:
            raise ValueError("load_id is required")
        
        # Get Neptune cluster endpoint from environment
        neptune_cluster = os.environ.get('NEPTUNE_CLUSTER_ENDPOINT')
        if not neptune_cluster:
            raise ValueError("NEPTUNE_CLUSTER_ENDPOINT environment variable not set")
        
        # Neptune bulk load status API endpoint
        neptune_endpoint = f"https://{neptune_cluster}:8182"
        status_url = f"{neptune_endpoint}/loader/{load_id}"
        
        # Check load status
        logger.info(f"Checking Neptune bulk load status for ID: {load_id}")
        
        response = requests.get(
            status_url,
            headers={'Content-Type': 'application/json'},
            timeout=30
        )
        
        if response.status_code != 200:
            raise Exception(f"Failed to check load status: {response.status_code} - {response.text}")
        
        status_result = response.json()
        payload = status_result.get('payload', {})
        
        overall_status = payload.get('overallStatus', {})
        status = overall_status.get('status', 'UNKNOWN')
        
        logger.info(f"Load {load_id} status: {status}")
        
        # Log additional details for monitoring
        if 'fullUri' in overall_status:
            logger.info(f"Load details: {overall_status['fullUri']}")
        
        if status in ['LOAD_FAILED', 'LOAD_CANCELLED']:
            # Log error details
            errors = payload.get('errorLogs', [])
            if errors:
                logger.error(f"Load errors: {errors}")
        
        return {
            'statusCode': 200,
            'body': {
                'load_id': load_id,
                'status': status,
                'details': payload
            }
        }
        
    except Exception as e:
        logger.exception("Failed to check Neptune bulk load status")
        return {
            'statusCode': 500,
            'body': {
                'error': str(e),
                'load_id': event.get('load_id'),
                'status': 'CHECK_FAILED'
            }
        }
