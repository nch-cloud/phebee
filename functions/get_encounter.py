import json
from phebee.utils.sparql import get_encounter

def lambda_handler(event, context):
    try:
        body = json.loads(event["body"])

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
                "statusCode": 200,
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