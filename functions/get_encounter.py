import json
from phebee.utils.sparql import get_encounter
from phebee.utils.aws import extract_body
from aws_lambda_powertools import Metrics, Logger, Tracer

logger = Logger()
tracer = Tracer()
metrics = Metrics()

def lambda_handler(event, context):
    try:
        body = extract_body(event)

        logger.info(body)

        subject_iri = body.get("subject_iri")
        encounter_id = body.get("encounter_id")

        if not subject_iri or not encounter_id:
            return {
                "statusCode": 400,
                "body": json.dumps({"message": "Missing required fields: subject_iri and encounter_id"})
            }

        result = get_encounter(subject_iri, encounter_id)

        if not result:
            return {
                "statusCode": 404,
                "body": json.dumps({"message": f"Encounter not found: {encounter_id} for subject: {subject_iri}"})
            }

        return {
            "statusCode": 200,
            "body": json.dumps(result)
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"message": f"Error retrieving encounter: {str(e)}"})
        }