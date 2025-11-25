import json
from aws_lambda_powertools import Metrics, Logger, Tracer
from phebee.utils.aws import extract_body
from phebee.utils.iceberg import get_evidence_record

logger = Logger()
tracer = Tracer()
metrics = Metrics()

def lambda_handler(event, context):
    logger.info(event)

    try:
        body = extract_body(event)
        evidence_id = body.get("evidence_id")

        if not evidence_id:
            return {
                "statusCode": 400,
                "body": json.dumps({"message": "evidence_id is required"}),
                "headers": {"Content-Type": "application/json"}
            }

        result = get_evidence_record(evidence_id)

        if result is None:
            return {
                "statusCode": 404,
                "body": json.dumps({"message": "Evidence not found"}),
                "headers": {"Content-Type": "application/json"}
            }

        return {
            "statusCode": 200,
            "body": json.dumps(result),
            "headers": {"Content-Type": "application/json"}
        }

    except Exception as e:
        logger.exception("Failed to get evidence")
        return {
            "statusCode": 500,
            "body": json.dumps({"message": f"Internal server error: {str(e)}"}),
            "headers": {"Content-Type": "application/json"}
        }
