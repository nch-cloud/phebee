import json
from aws_lambda_powertools import Metrics, Logger, Tracer
from phebee.utils.aws import extract_body
from phebee.utils.sparql import create_project

logger = Logger()
tracer = Tracer()
metrics = Metrics()

def lambda_handler(event, context):
    logger.info(event)
    body = extract_body(event)

    project_id = body.get("project_id")
    project_label = body.get("project_label")

    if not project_id or not project_label:
        return {
            "statusCode": 400,
            "body": json.dumps({"message": "Missing required fields: project_id and project_label"}),
            "headers": {"Content-Type": "application/json"}
        }

    created = create_project(project_id, project_label)

    message = (
        "Project created."
        if created else
        "Project already exists."
    )

    return {
        "statusCode": 200,
        "body": json.dumps({
            "project_created": created,
            "project_id": project_id,
            "project_iri": f"http://ods.nationwidechildrens.org/phebee/projects/{project_id}",
            "message": message
        }),
        "headers": {"Content-Type": "application/json"}
    }