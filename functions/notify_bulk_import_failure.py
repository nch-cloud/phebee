import json
import logging
from datetime import datetime
from phebee.utils.eventbridge import fire_event

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

        # Fire bulk import failure event
        event_data = {
            "run_id": run_id,
            "timestamp": failure_time,
            "error": error
        }

        fire_event("bulk_import_failure", event_data)
        logger.info(f"Fired bulk_import_failure event for run {run_id}")

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
