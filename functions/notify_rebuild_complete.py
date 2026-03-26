"""
Notify Rebuild Complete Lambda

Logs rebuild statistics and sends EventBridge event for successful rebuild completion.
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
    Log rebuild completion statistics and send EventBridge notification.

    Input:
        {
            "status": "SUCCESS",
            "state": {
                "validation": {"Payload": {"runId": "..."}},
                "rebuildEvidence": true/false,
                "rebuildIceberg": true/false,
                "rebuildNeptune": true/false,
                "rehashResult": {...},
                "materializeResult": {...},
                "ttlResult": {...},
                "rehashValidation": {"Payload": {...}},
                "materializationValidation": {"Payload": {...}},
                "neptuneValidation": {"Payload": {...}}
            }
        }
    """
    try:
        state = event.get('state', {})

        # Extract run ID from validation payload
        run_id = state.get('validation', {}).get('Payload', {}).get('runId')

        # Extract rebuild flags
        rebuild_evidence = state.get('rebuildEvidence', False)
        rebuild_iceberg = state.get('rebuildIceberg', False)
        rebuild_neptune = state.get('rebuildNeptune', False)

        logger.info("=" * 80)
        logger.info("REBUILD COMPLETED SUCCESSFULLY")
        logger.info("=" * 80)
        logger.info(f"Run ID: {run_id}")
        logger.info(f"Rebuild Evidence: {rebuild_evidence}")
        logger.info(f"Rebuild Iceberg: {rebuild_iceberg}")
        logger.info(f"Rebuild Neptune: {rebuild_neptune}")

        # Log EMR job results if present
        rehash_result = state.get('rehashResult')
        if rehash_result:
            logger.info(f"Rehash Job ID: {rehash_result.get('JobRunId')}")

        materialize_result = state.get('materializeResult')
        if materialize_result:
            logger.info(f"Materialize Job ID: {materialize_result.get('JobRunId')}")

        ttl_result = state.get('ttlResult')
        if ttl_result:
            logger.info(f"TTL Generation Job ID: {ttl_result.get('JobRunId')}")

        # Log validation summaries
        validations = {}
        if state.get('rehashValidation'):
            validations['rehash'] = state['rehashValidation'].get('Payload', {})
        if state.get('materializationValidation'):
            validations['materialization'] = state['materializationValidation'].get('Payload', {})
        if state.get('neptuneValidation'):
            validations['neptune'] = state['neptuneValidation'].get('Payload', {})

        if validations:
            logger.info("Validation Results:")
            for validation_name, validation_result in validations.items():
                if validation_result:
                    logger.info(f"  {validation_name}: {'PASSED' if validation_result.get('valid') else 'N/A'}")

        logger.info("=" * 80)

        # Send EventBridge event
        detail = {
            "runId": run_id,
            "status": "SUCCESS",
            "timestamp": datetime.utcnow().isoformat(),
            "rebuildFlags": {
                "rebuildEvidence": rebuild_evidence,
                "rebuildIceberg": rebuild_iceberg,
                "rebuildNeptune": rebuild_neptune
            },
            "validations": validations
        }

        response = events.put_events(
            Entries=[
                {
                    'Source': 'phebee.rebuild',
                    'DetailType': 'RebuildCompleted',
                    'Detail': json.dumps(detail),
                    'EventBusName': EVENT_BUS_NAME
                }
            ]
        )

        logger.info(f"Sent EventBridge notification: {response}")

        return {
            "statusCode": 200,
            "body": {
                "message": "Rebuild completed successfully",
                "runId": run_id
            }
        }

    except Exception as e:
        logger.error(f"Error in notify complete: {str(e)}", exc_info=True)
        raise
