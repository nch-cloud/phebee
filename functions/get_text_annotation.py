import json
from phebee.utils.sparql import get_text_annotation
from phebee.utils.aws import extract_body

def lambda_handler(event, context):
    try:
        body = extract_body(event)
        annotation_iri = body.get("annotation_iri")

        if not annotation_iri:
            return {
                "statusCode": 400,
                "body": json.dumps(
                    {"message": "Missing required field: annotation_iri"}
                ),
            }

        result = get_text_annotation(annotation_iri)

        if not result:
            return {
                "statusCode": 200,
                "body": json.dumps({"message": "TextAnnotation not found"}),
            }

        return {"statusCode": 200, "body": json.dumps(result)}

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps(
                {"message": f"Error retrieving text annotation: {str(e)}"}
            ),
        }
