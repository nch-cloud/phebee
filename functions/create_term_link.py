import json
from phebee.utils.sparql import create_term_link

def lambda_handler(event, context):
    try:
        body = json.loads(event["body"])

        source_node_iri = body.get("source_node_iri")
        term_iri = body.get("term_iri")
        creator_iri = body.get("creator_iri")
        evidence_iris = body.get("evidence_iris")

        if not source_node_iri or not term_iri or not creator_iri or not evidence_iris:
            return {
                "statusCode": 400,
                "body": json.dumps({
                    "message": "Missing required field(s): source_node_iri, term_iri, creator_iri, and evidence_iris are all required."
                })
            }

        if not isinstance(evidence_iris, list) or not evidence_iris:
            return {
                "statusCode": 400,
                "body": json.dumps({
                    "message": "evidence_iris must be a non-empty list"
                })
            }

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