import json
from phebee.utils.sparql import get_term_link
from phebee.utils.aws import extract_body

def lambda_handler(event, context):
    try:
        body = extract_body(event)
        termlink_iri = body.get("termlink_iri")

        if not termlink_iri:
            return {
                "statusCode": 400,
                "body": json.dumps({"message": "Missing required field: termlink_iri"})
            }

        result = get_term_link(termlink_iri)

        if not result:
            return {
                "statusCode": 200,
                "body": json.dumps({"message": "TermLink not found"})
            }

        return {
            "statusCode": 200,
            "body": json.dumps(result)
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"message": f"Error retrieving TermLink: {str(e)}"})
        }