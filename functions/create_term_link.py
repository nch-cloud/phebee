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

        if not source_node_iri or not term_iri or not creator_iri or "evidence_iris" not in body:
            return {
                "statusCode": 400,
                "body": json.dumps({
                    "message": "Missing required field(s): source_node_iri, term_iri, creator_iri, and evidence_iris are all required."
                })
            }

        logger.info(f"Creating link from source {source_node_iri} to term: {term_iri}")

        termlink_iri = create_term_link(
            source_node_iri=source_node_iri,
            term_iri=term_iri,
            creator_iri=creator_iri,
            evidence_iris=evidence_iris
        )

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "TermLink created",
                "termlink_iri": termlink_iri
            })
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"message": f"Error creating TermLink: {str(e)}"})
        }