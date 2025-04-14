from aws_lambda_powertools import Metrics, Logger, Tracer
from phebee.constants import PHEBEE
from phebee.utils.neptune import execute_update
from phebee.utils.sparql import node_exists
from phebee.utils.aws import extract_body
import json

logger = Logger()
tracer = Tracer()
metrics = Metrics()


def lambda_handler(event, context):
    logger.info(event)

    body = extract_body(event)

    project_id = body.get("project_id")
    project_label = body.get("project_label")

    # Check if the project already exists
    project_iri = f"{PHEBEE}/projects/{project_id}"

    project_exists = node_exists(project_iri)

    logger.info(f"Project {project_iri} exists: {project_exists}")

    if project_exists:
        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "project_created": False,
                    "message": f"Project already exists with id {project_id}",
                }
            ),
            "headers": {"Content-Type": "application/json"},
        }
    else:
        # Add a new node to a named graph specific to the project being created.
        # Add label and project id properties to the project node.
        sparql = f"""
            PREFIX phebee: <{PHEBEE}>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

            INSERT DATA
            {{
                GRAPH <{PHEBEE}/projects/{project_id}>
                {{
                    phebee:\/projects\/{project_id} rdf:type phebee:\#Project .
                    phebee:\/projects\/{project_id} rdfs:label "{project_label}" .
                    phebee:\/projects\/{project_id} phebee:\#hasProjectId "{project_id}"
                }}
            }}
        """

        execute_update(sparql)
        return {
            "statusCode": 200,
            "body": json.dumps({"project_created": True}),
            "headers": {"Content-Type": "application/json"},
        }
