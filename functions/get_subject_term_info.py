import json
from phebee.utils.iceberg import get_subject_term_info
from phebee.utils.aws import extract_body

def lambda_handler(event, context):
    try:
        body = extract_body(event)
        subject_iri = body.get("subject_iri")
        term_iri = body.get("term_iri")
        qualifiers = body.get("qualifiers", [])

        if not subject_iri:
            return {
                "statusCode": 400,
                "body": json.dumps({"message": "Missing required field: subject_iri"})
            }

        if not term_iri:
            return {
                "statusCode": 400,
                "body": json.dumps({"message": "Missing required field: term_iri"})
            }

        # Extract subject_id from subject_iri
        subject_id = subject_iri.split("/")[-1]
        
        result = get_subject_term_info(subject_id, term_iri, qualifiers)

        if not result:
            return {
                "statusCode": 404,
                "body": json.dumps({"message": "Term not found for subject"})
            }

        return {
            "statusCode": 200,
            "body": json.dumps(result)
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"message": f"Internal server error: {str(e)}"})
        }
