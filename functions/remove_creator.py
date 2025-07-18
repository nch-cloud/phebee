import json
from phebee.utils.sparql import delete_creator
from phebee.utils.aws import extract_body

def lambda_handler(event, context):
    try:
        body = extract_body(event)
        creator_id = body.get("creator_id")
        creator_iri = body.get("creator_iri")
        
        if not creator_id and not creator_iri:
            return {
                "statusCode": 400,
                "body": json.dumps({"message": "Missing required field: either creator_id or creator_iri"})
            }
        
        # Use creator_iri if provided, otherwise use creator_id
        creator_id_or_iri = creator_iri if creator_iri else creator_id
        
        # Delete the creator and get the actual IRI that was deleted
        deleted_iri = delete_creator(creator_id_or_iri)

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Creator deleted",
                "creator_iri": deleted_iri
            })
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"message": f"Error deleting creator: {str(e)}"})
        }