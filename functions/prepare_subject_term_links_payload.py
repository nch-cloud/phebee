import json
from aws_lambda_powertools import Logger, Tracer, Metrics

logger = Logger()
tracer = Tracer()
metrics = Metrics()


def lambda_handler(event, context):
    logger.info(f"Received event: {json.dumps(event)}")

    term_links_payload = event.get("term_links_payload", [])
    source_node_iri = event.get("source_node_iri")

    if not term_links_payload or not source_node_iri:
        return {
            "statusCode": 400,
            "body": json.dumps(
                {
                    "message": "Missing required fields: term_links_payload and source_node_iri"
                }
            ),
        }

    fixed_term_links_payload = []

    for link in term_links_payload:
        updated_link = dict(link)
        updated_link["source_node_iri"] = source_node_iri
        fixed_term_links_payload.append(updated_link)

    return {"statusCode": 200, "fixed_term_links_payload": fixed_term_links_payload}
