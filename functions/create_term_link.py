import json
from phebee.utils.sparql import create_term_link
from phebee.utils.aws import extract_body
from aws_lambda_powertools import Metrics, Logger, Tracer

logger = Logger()
tracer = Tracer()
metrics = Metrics()

def lambda_handler(event, context):
    try:
        logger.info(event)
        body = extract_body(event)

        subject_id = body.get("subject_id")
        term_iri = body.get("term_iri")
        creator_id = body.get("creator_id")
        qualifiers = body.get("qualifiers", [])  # Add support for qualifiers

        if not subject_id or not term_iri or not creator_id:
            return {
                "statusCode": 400,
                "body": json.dumps({
                    "message": "Missing required field(s): subject_id, term_iri, and creator_id are all required."
                })
            }

        logger.info("Creating link from subject %s to term: %s", subject_id, term_iri)
        if qualifiers:
            logger.info("With qualifiers: %s", qualifiers)

        # Convert subject_id to source_node_iri for SPARQL function
        source_node_iri = f"http://ods.nationwidechildrens.org/phebee/subjects/{subject_id}"
        creator_iri = f"http://ods.nationwidechildrens.org/phebee/creators/{creator_id}"

        result = create_term_link(
            subject_iri=source_node_iri,
            term_iri=term_iri,
            creator_iri=creator_iri,
            qualifiers=qualifiers  # Pass qualifiers to the function
        )
        
        termlink_iri = result["termlink_iri"]
        is_new = result["created"]
        
        message = "TermLink created" if is_new else "Existing TermLink reused"

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": message,
                "termlink_iri": termlink_iri,
                "created": is_new
            })
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"message": "Error creating TermLink: %s" % str(e)})
        }