import json
from phebee.utils.sparql import create_creator
from phebee.utils.aws import extract_body
from urllib.parse import quote

def lambda_handler(event, context):
    try:
        body = extract_body(event)

        creator_id = body.get("creator_id")
        creator_type = body.get("creator_type")
        name = body.get("name")
        version = body.get("version")

        if not creator_id or not creator_type:
            return {
                "statusCode": 400,
                "body": json.dumps({"message": "Missing required fields: creator_id and creator_type"})
            }

        if creator_type not in ("human", "automated"):
            return {
                "statusCode": 400,
                "body": json.dumps({"message": "Invalid creator_type. Must be 'human' or 'automated'"})
            }

        if creator_type == "automated" and not version:
            return {
                "statusCode": 400,
                "body": json.dumps({"message": "Missing required field: version for automated creator"})
            }

        # Create the creator and get the IRI
        creator_iri = create_creator(
            creator_id=creator_id,
            creator_type=creator_type,
            name=name,
            version=version
        )
        
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Creator created",
                "creator_iri": creator_iri
            })
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"message": f"Error creating creator: {str(e)}"})
        }