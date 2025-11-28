import json
import logging
from datetime import datetime
from phebee.utils.eventbridge import fire_event

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """Notify completion of bulk import process"""
    logger.info(event)
    
    try:
        run_id = event.get('run_id')
        status = event.get('status', 'SUCCESS')
        emr_job_id = event.get('emr_job_id')
        load_id = event.get('load_id')
        domain_load_id = event.get('domain_load_id')
        prov_load_id = event.get('prov_load_id')
        
        completion_time = datetime.utcnow().isoformat()
        
        # Log completion
        logger.info(f"Bulk import completed successfully for run {run_id}")
        logger.info(f"EMR Job ID: {emr_job_id}")
        logger.info(f"Neptune Load ID: {load_id}")
        
        # Fire bulk import success event
        event_data = {
            "run_id": run_id,
            "timestamp": completion_time,
            "domain_load_id": domain_load_id,
            "prov_load_id": prov_load_id
        }
        
        fire_event("bulk_import_success", event_data)
        logger.info(f"Fired bulk_import_success event for run {run_id}")
        
        return {
            'statusCode': 200,
            'body': {
                'message': 'Bulk import completed successfully',
                'run_id': run_id,
                'status': status,
                'completion_time': completion_time,
                'emr_job_id': emr_job_id,
                'neptune_load_id': load_id
            }
        }
        
    except Exception as e:
        logger.exception("Failed to process completion notification")
        return {
            'statusCode': 500,
            'body': {
                'error': str(e),
                'run_id': event.get('run_id')
            }
        }
