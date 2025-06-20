import json
from phebee.utils.sparql import get_clinical_note
from phebee.utils.aws import extract_body
from aws_lambda_powertools import Metrics, Logger, Tracer

logger = Logger()
tracer = Tracer()
metrics = Metrics()

def lambda_handler(event, context):
    try:
        body = extract_body(event)

        logger.info(f"body: {body}")

        encounter_iri = body.get("encounter_iri")
        clinical_note_id = body.get("clinical_note_id")

        if not encounter_iri or not clinical_note_id:
            return {
                "statusCode": 400,
                "body": json.dumps({"message": "Missing required fields: encounter_iri and clinical_note_id"})
            }

        result = get_clinical_note(encounter_iri, clinical_note_id)

        logger.info(result)

        if not result:
            return {
                "statusCode": 200,
                "body": json.dumps({"message": "ClinicalNote not found"})
            }

        return {
            "statusCode": 200,
            "body": json.dumps(result)
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"message": f"Error retrieving clinical note: {str(e)}"})
        }