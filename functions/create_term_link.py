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

        source_node_iri = body.get("source_node_iri")
        term_iri = body.get("term_iri")
        creator_iri = body.get("creator_iri")
        evidence_iris = body.get("evidence_iris")
        qualifiers = body.get("qualifiers", [])  # Add support for qualifiers

        if not source_node_iri or not term_iri or not creator_iri or "evidence_iris" not in body:
            return {
                "statusCode": 400,
                "body": json.dumps({
                    "message": "Missing required field(s): source_node_iri, term_iri, creator_iri, and evidence_iris are all required."
                })
            }

        logger.info("Creating link from source %s to term: %s", source_node_iri, term_iri)
        if qualifiers:
            logger.info("With qualifiers: %s", qualifiers)

        result = create_term_link(
            source_node_iri=source_node_iri,
            term_iri=term_iri,
            creator_iri=creator_iri,
            evidence_iris=evidence_iris,
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