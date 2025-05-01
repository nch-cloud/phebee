import json
from phebee.utils.sparql import delete_term_link

def lambda_handler(event, context):
    try:
        body = json.loads(event["body"])
        termlink_iri = body.get("termlink_iri")

        if not termlink_iri:
            return {
                "statusCode": 400,
                "body": json.dumps({"message": "Missing required field: termlink_iri"})
            }

        delete_term_link(termlink_iri)

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "TermLink deleted",
                "termlink_iri": termlink_iri
            })
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"message": f"Error deleting TermLink: {str(e)}"})
        }