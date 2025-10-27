import json
from datetime import datetime
from phebee.utils.eventbridge import fire_event


def lambda_handler(event, context):
    """Fire bulk import success or failure events."""
    
    event_type = event.get('event_type')
    run_id = event.get('run_id')
    
    if not event_type or not run_id:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "event_type and run_id are required"})
        }
    
    # Prepare event data
    event_data = {
        "run_id": run_id,
        "timestamp": datetime.utcnow().isoformat()
    }
    
    # Add additional context for failure events
    if event_type == "BULK_IMPORT_FAILURE":
        event_data.update({
            "domain_status": event.get('domain_status'),
            "prov_status": event.get('prov_status'),
            "error": event.get('error')
        })
    elif event_type == "BULK_IMPORT_SUCCESS":
        event_data.update({
            "domain_load_id": event.get('domain_load_id'),
            "prov_load_id": event.get('prov_load_id')
        })
    
    try:
        fire_event(event_type.lower(), event_data)
        
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": f"Event {event_type} fired successfully",
                "run_id": run_id
            })
        }
        
    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({
                "error": f"Failed to fire event: {str(e)}",
                "run_id": run_id
            })
        }
