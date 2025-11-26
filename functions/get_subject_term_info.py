import json
from phebee.utils.iceberg import get_subject_term_info
from phebee.utils.aws import extract_body

def lambda_handler(event, context):
    try:
        body = extract_body(event)
        subject_id = body.get("subject_id")
        term_iri = body.get("term_iri")
        qualifiers = body.get("qualifiers", [])

        if not subject_id:
            return {
                "statusCode": 400,
                "body": json.dumps({"message": "Missing required field: subject_id"})
            }

        if not term_iri:
            return {
                "statusCode": 400,
                "body": json.dumps({"message": "Missing required field: term_iri"})
            }

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
