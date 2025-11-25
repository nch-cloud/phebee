import json
import logging
from phebee.utils.neptune import get_load_status

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """Check Neptune bulk load status using existing Neptune utilities"""
    logger.info(event)
    
    try:
        load_id = event.get('load_id')
        
        if not load_id:
            raise ValueError("load_id is required")
        
        logger.info(f"Checking Neptune bulk load status for ID: {load_id}")
        
        # Use existing Neptune utility to get load status
        status_result = get_load_status(load_id)
        
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
