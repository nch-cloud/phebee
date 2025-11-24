import json
import logging
import boto3
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """Notify failure of bulk import process"""
    logger.info(event)
    
    try:
        run_id = event.get('run_id')
        status = event.get('status', 'FAILED')
        error = event.get('error', {})
        
        failure_time = datetime.utcnow().isoformat()
        
        # Log failure details
        logger.error(f"Bulk import failed for run {run_id}")
        logger.error(f"Error details: {error}")
        
        # Could send SNS notification, update DynamoDB, etc.
        # For now, just return failure info
        
        return {
            'statusCode': 200,
            'body': {
                'message': 'Bulk import failed',
                'run_id': run_id,
                'status': status,
                'failure_time': failure_time,
                'error': error
            }
        }
        
    except Exception as e:
        logger.exception("Failed to process failure notification")
        return {
            'statusCode': 500,
            'body': {
                'error': str(e),
                'run_id': event.get('run_id')
            }
        }
