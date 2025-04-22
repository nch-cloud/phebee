import json
from phebee.utils.sparql import delete_clinical_note

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

        delete_clinical_note(encounter_iri, clinical_note_id)

        note_iri = f"{encounter_iri}/note/{clinical_note_id}"

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "ClinicalNote deleted",
                "clinical_note_iri": note_iri
            })
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"message": f"Error deleting clinical note: {str(e)}"})
        }