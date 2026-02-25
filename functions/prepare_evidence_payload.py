"""Prepare evidence payload by replacing temporary subject identifier with actual UUID."""

import json
from aws_lambda_powertools import Logger, Tracer, Metrics

logger = Logger()
tracer = Tracer()
metrics = Metrics()


def lambda_handler(event, context):
    logger.info("Received event: %s", json.dumps(event))

    evidence_payload = event.get("evidence_payload", [])
    subject_id = event.get("subject_id")

    if not evidence_payload or not subject_id:
        return {
            "statusCode": 400,
            "body": json.dumps(
                {
                    "message": "Missing required fields: evidence_payload and subject_id"
                }
            ),
        }

    fixed_evidence_payload = []

    for evidence in evidence_payload:
        updated_evidence = dict(evidence)
        # Replace temporary subject_id with actual UUID from CreateSubject response
        updated_evidence["subject_id"] = subject_id
        fixed_evidence_payload.append(updated_evidence)

    return {"statusCode": 200, "fixed_evidence_payload": fixed_evidence_payload}
