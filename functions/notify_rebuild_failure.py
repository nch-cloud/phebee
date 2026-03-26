"""
Notify Rebuild Failure Lambda

Logs error details and sends EventBridge event for failed rebuild.
"""

import json
import os
import logging
import boto3
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

events = boto3.client('events')
EVENT_BUS_NAME = os.environ.get('EventBusName', 'default')


def lambda_handler(event, context):
    """
    Log rebuild failure details and send EventBridge notification.

    Input:
        {
            "status": "FAILED",
            "state": {
                "validation": {"Payload": {"runId": "...", "validated": false, "error": "..."}},
                "rebuildEvidence": true/false,
                "rebuildIceberg": true/false,
                "rebuildNeptune": true/false,
                "error": {...}  // Catch error if exception occurred
            }
        }
    """
    try:
        state = event.get('state', {})

        # Extract run ID from validation payload (might be None if validation failed early)
        run_id = None
        validation = state.get('validation', {})
        if validation:
            run_id = validation.get('Payload', {}).get('runId')

        # Extract rebuild flags
        rebuild_evidence = state.get('rebuildEvidence', False)
        rebuild_iceberg = state.get('rebuildIceberg', False)
        rebuild_neptune = state.get('rebuildNeptune', False)

        logger.error("=" * 80)
        logger.error("REBUILD FAILED")
        logger.error("=" * 80)
        logger.error(f"Run ID: {run_id}")
        logger.error(f"Rebuild Evidence: {rebuild_evidence}")
        logger.error(f"Rebuild Iceberg: {rebuild_iceberg}")
        logger.error(f"Rebuild Neptune: {rebuild_neptune}")

        # Log error details - could be from validation or from caught exception
        error = {}

        # Check if validation failed
        if validation and not validation.get('Payload', {}).get('validated', False):
            validation_error = validation.get('Payload', {}).get('error')
            if validation_error:
                error = {"validationError": validation_error}
                logger.error(f"Validation Error: {validation_error}")

        # Check if there was a caught exception
        caught_error = state.get('error', {})
        if caught_error:
            error = {**error, "caughtError": caught_error}
            logger.error(f"Caught Error: {json.dumps(caught_error, indent=2)}")

        # Check if any validation step failed
        if state.get('rehashValidation', {}).get('Payload', {}).get('valid') == False:
            error = {**error, "rehashValidationFailed": state['rehashValidation']['Payload']}
            logger.error("Rehash validation failed")

        if state.get('materializationValidation', {}).get('Payload', {}).get('valid') == False:
            error = {**error, "materializationValidationFailed": state['materializationValidation']['Payload']}
            logger.error("Materialization validation failed")

        if state.get('neptuneValidation', {}).get('Payload', {}).get('valid') == False:
            error = {**error, "neptuneValidationFailed": state['neptuneValidation']['Payload']}
            logger.error("Neptune validation failed")

        if not error:
            logger.error("No error details available")

        logger.error("=" * 80)

        # Send EventBridge event
        detail = {
            "runId": run_id,
            "status": "FAILED",
            "timestamp": datetime.utcnow().isoformat(),
            "rebuildFlags": {
                "rebuildEvidence": rebuild_evidence,
                "rebuildIceberg": rebuild_iceberg,
                "rebuildNeptune": rebuild_neptune
            },
            "error": error
        }

        response = events.put_events(
            Entries=[
                {
                    'Source': 'phebee.rebuild',
                    'DetailType': 'RebuildFailed',
                    'Detail': json.dumps(detail),
                    'EventBusName': EVENT_BUS_NAME
                }
            ]
        )

        logger.info(f"Sent EventBridge notification: {response}")

        return {
            "statusCode": 500,
            "body": {
                "message": "Rebuild failed",
                "runId": run_id,
                "error": error
            }
        }

    except Exception as e:
        logger.error(f"Error in notify failure: {str(e)}", exc_info=True)
        raise
