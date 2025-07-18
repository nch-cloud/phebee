import json
from phebee.utils.sparql import create_text_annotation
from phebee.utils.aws import extract_body

def lambda_handler(event, context):
    try:
        body = extract_body(event)

        text_source_iri = body.get("text_source_iri")
        span_start = body.get("span_start")
        span_end = body.get("span_end")
        creator_iri = body.get("creator_iri")
        term_iri = body.get("term_iri")
        metadata = body.get("metadata")

        if not text_source_iri:
            return {
                "statusCode": 400,
                "body": json.dumps({"message": "Missing required field: text_source_iri"})
            }

        annotation_iri = create_text_annotation(
            text_source_iri=text_source_iri,
            span_start=span_start,
            span_end=span_end,
            creator_iri=creator_iri,
            term_iri=term_iri,
            metadata=metadata
        )

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "TextAnnotation created",
                "annotation_iri": annotation_iri
            })
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"message": f"Error creating text annotation: {str(e)}"})
        }