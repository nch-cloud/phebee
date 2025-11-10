import json
from phebee.utils.sparql import create_clinical_note
from phebee.utils.aws import extract_body

def lambda_handler(event, context):
    try:
        body = extract_body(event)

        encounter_iri = body.get("encounter_iri")
        clinical_note_id = body.get("clinical_note_id")
        note_timestamp = body.get("note_timestamp")  # Optional
        provider_type = body.get("provider_type")  # Optional
        author_specialty = body.get("author_specialty")  # Optional
        note_type = body.get("note_type")  # Optional

        if not encounter_iri or not clinical_note_id:
            return {
                "statusCode": 400,
                "body": json.dumps({"message": "Missing required fields: encounter_iri and clinical_note_id"})
            }

        create_clinical_note(encounter_iri, clinical_note_id, note_timestamp, provider_type, author_specialty, note_type)

        note_iri = f"{encounter_iri}/note/{clinical_note_id}"

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "ClinicalNote created",
                "clinical_note_iri": note_iri
            })
        }

    except Exception as e:
        error_message = "Error creating clinical note: %s" % str(e)
        return {
            "statusCode": 500,
            "body": json.dumps({"message": error_message})
        }