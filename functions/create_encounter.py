import json
from phebee.utils.sparql import create_encounter

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

        encounter_iri = f"{subject_iri}/encounter/{encounter_id}"
        create_encounter(subject_iri, encounter_id)

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Encounter created",
                "encounter_iri": encounter_iri
            })
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"message": f"Error creating encounter: {str(e)}"})
        }