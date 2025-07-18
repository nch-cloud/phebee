import json
from phebee.utils.sparql import get_creator
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

        result = get_creator(creator_id)

        if not result:
            return {
                "statusCode": 200,
                "body": json.dumps({"message": "Creator not found"})
            }

        return {
            "statusCode": 200,
            "body": json.dumps(result)
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"message": f"Error retrieving creator: {str(e)}"})
        }