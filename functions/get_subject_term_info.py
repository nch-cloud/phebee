import json
from phebee.utils.iceberg import get_subject_term_info
from phebee.utils.aws import extract_body
from aws_lambda_powertools import Logger, Tracer, Metrics

logger = Logger()
tracer = Tracer()
metrics = Metrics()

def lambda_handler(event, context):
    logger.info(f"Event: {event}")

    try:
        body = extract_body(event)
        subject_id = body.get("subject_id")
        term_iri = body.get("term_iri")
        qualifiers = body.get("qualifiers", [])

        logger.info(f"Parameters: subject_id={subject_id}, term_iri={term_iri}, qualifiers={qualifiers}")

        if not subject_id:
            return {
                "statusCode": 400,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"message": "Missing required field: subject_id"})
            }

        if not term_iri:
            return {
                "statusCode": 400,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"message": "Missing required field: term_iri"})
            }

        logger.info("Calling get_subject_term_info...")
        result = get_subject_term_info(subject_id, term_iri, qualifiers)
        logger.info(f"Result: {result}")

        if not result:
            return {
                "statusCode": 404,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"message": "Term not found for subject"})
            }

        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(result)
        }

    except Exception as e:
        logger.error(f"Error in get_subject_term_info: {str(e)}", exc_info=True)
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"message": f"Internal server error: {str(e)}"})
        }
