"""
Validate Rebuild Input Lambda

Validates input parameters for rebuild materialized data state machine.
Generates a run_id (UUID) if not provided.
"""

import json
import uuid
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    """
    Validate rebuild input parameters and generate run_id if needed.

    Input:
        {
            "rebuildEvidence": true/false,
            "rebuildIceberg": true/false,
            "rebuildNeptune": true/false,
            "runId": "optional-uuid"
        }

    Output:
        {
            "runId": "generated-or-provided-uuid",
            "rebuildEvidence": true/false,
            "rebuildIceberg": true/false,
            "rebuildNeptune": true/false,
            "validated": true/false,
            "error": "error message if validation fails"
        }
    """
    try:
        logger.info(f"Validating rebuild input: {json.dumps(event)}")

        rebuild_evidence = event.get('rebuildEvidence', False)
        rebuild_iceberg = event.get('rebuildIceberg', False)
        rebuild_neptune = event.get('rebuildNeptune', False)
        run_id = event.get('runId')

        # Validation 1: At least one rebuild flag must be true
        if not (rebuild_evidence or rebuild_iceberg or rebuild_neptune):
            error_msg = "At least one rebuild flag must be true (rebuildEvidence, rebuildIceberg, or rebuildNeptune)"
            logger.error(error_msg)
            return {
                "validated": False,
                "error": error_msg,
                "runId": run_id
            }

        # Validation 2: Generate run_id if not provided
        if not run_id:
            run_id = str(uuid.uuid4())
            logger.info(f"Generated new run_id: {run_id}")
        else:
            logger.info(f"Using provided run_id: {run_id}")

        # Validation 3: Verify run_id is valid UUID format
        try:
            uuid.UUID(run_id)
        except ValueError:
            error_msg = f"Invalid run_id format: {run_id}. Must be a valid UUID."
            logger.error(error_msg)
            return {
                "validated": False,
                "error": error_msg,
                "runId": run_id
            }

        logger.info(f"Validation passed. Run ID: {run_id}")
        logger.info(f"  rebuildEvidence: {rebuild_evidence}")
        logger.info(f"  rebuildIceberg: {rebuild_iceberg}")
        logger.info(f"  rebuildNeptune: {rebuild_neptune}")

        return {
            "validated": True,
            "runId": run_id,
            "rebuildEvidence": rebuild_evidence,
            "rebuildIceberg": rebuild_iceberg,
            "rebuildNeptune": rebuild_neptune
        }

    except Exception as e:
        logger.error(f"Unexpected error during validation: {str(e)}", exc_info=True)
        return {
            "validated": False,
            "error": f"Unexpected error: {str(e)}",
            "runId": event.get('runId')
        }
