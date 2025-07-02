import json
from phebee.utils.sparql import delete_creator
from phebee.utils.aws import extract_body

def lambda_handler(event, context):
    try:
        body = extract_body(event)
        creator_id = body.get("creator_id")

        if not creator_id:
            return {
                "statusCode": 400,
                "body": json.dumps({"message": "Missing required field: creator_id"})
            }

        delete_creator(creator_id)

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Creator deleted",
                "creator_iri": f"http://ods.nationwidechildrens.org/phebee/creator/{creator_id}"
            })
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"message": f"Error deleting creator: {str(e)}"})
        }