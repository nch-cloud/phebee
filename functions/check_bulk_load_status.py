import json
from phebee.utils.neptune import get_load_status


def lambda_handler(event, context):
    """Check status of both domain and provenance Neptune loads."""
    
    domain_load_id = event.get('domain_load_id')
    prov_load_id = event.get('prov_load_id')
    run_id = event.get('run_id')
    
    if not all([domain_load_id, prov_load_id, run_id]):
        return {
            "status": "FAILED",
            "run_id": run_id,
            "domain_status": "UNKNOWN",
            "prov_status": "UNKNOWN",
            "error": "Missing required parameters"
        }
    
    try:
        domain_status = get_load_status(domain_load_id)
        prov_status = get_load_status(prov_load_id)
        
        domain_state = domain_status.get('payload', {}).get('overallStatus', {}).get('status')
        prov_state = prov_status.get('payload', {}).get('overallStatus', {}).get('status')
        
        # Check if both completed successfully
        if domain_state == 'LOAD_COMPLETED' and prov_state == 'LOAD_COMPLETED':
            return {
                "status": "SUCCESS",
                "run_id": run_id,
                "domain_load_id": domain_load_id,
                "prov_load_id": prov_load_id
            }
        
        # Check if either failed
        failed_states = ['LOAD_FAILED', 'LOAD_CANCELLED']
        if domain_state in failed_states or prov_state in failed_states:
            return {
                "status": "FAILED", 
                "run_id": run_id,
                "domain_status": domain_state,
                "prov_status": prov_state
            }
        
        # Still in progress
        return {
            "status": "PENDING",
            "run_id": run_id,
            "domain_status": domain_state,
            "prov_status": prov_state
        }
        
    except Exception as e:
        return {
            "status": "FAILED",
            "run_id": run_id,
            "domain_status": "UNKNOWN",
            "prov_status": "UNKNOWN",
            "error": str(e)
        }
