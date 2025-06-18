from aws_lambda_powertools import Metrics, Logger, Tracer
from phebee.constants import PHEBEE
from phebee.utils.neptune import execute_update
from phebee.utils.aws import extract_body
import json

logger = Logger()
tracer = Tracer()
metrics = Metrics()


def lambda_handler(event, context):
    logger.info(event)

    body = extract_body(event)

    project_id = body.get("project_id")

    sparql = f"""
        PREFIX phebee: <{PHEBEE}>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

        CLEAR GRAPH <{PHEBEE}/projects/{project_id}>
    """

    try:
        execute_update(sparql)
        return {
            "statusCode": 200,
            "body": json.dumps({"message": f"Project {project_id} successfully removed."}),
            "headers": {"Content-Type": "application/json"},
        }

    except Exception as e:
        logger.exception("Failed to remove project")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": f"Failed to remove project: {str(e)}"}),
            "headers": {"Content-Type": "application/json"},
        }