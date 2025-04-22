import json
from phebee.utils.sparql import get_clinical_note

def lambda_handler(event, context):
    try:
        body = json.loads(event["body"])

        encounter_iri = body.get("encounter_iri")
        clinical_note_id = body.get("clinical_note_id")

        if not encounter_iri or not clinical_note_id:
            return {
                "statusCode": 400,
                "body": json.dumps({"message": "Missing required fields: encounter_iri and clinical_note_id"})
            }

        result = get_clinical_note(encounter_iri, clinical_note_id)

        if not result["properties"]:
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