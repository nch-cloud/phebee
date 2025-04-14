from aws_lambda_powertools import Metrics, Logger, Tracer
from phebee.utils.neptune import execute_update
from phebee.utils.sparql import get_subject
from phebee.utils.aws import extract_body
import json

logger = Logger()
tracer = Tracer()
metrics = Metrics()


def lambda_handler(event, context):
    logger.info(event)

    body = extract_body(event)

    project_id = body.get("project_id")
    project_subject_id = body.get("project_subject_id")

    subject = get_subject(project_id, project_subject_id)
    print(subject)

    # TODO - Check for statements that will currently be orphaned.  Perhaps add a cascade flag to either prevent subject deletion or propagate it.

    # Remove triples with subject as subject
    sparql = f"""
        DELETE WHERE {{
            <{subject["iri"]}> ?p ?o .
        }}
    """

    execute_update(sparql)

    # Remove triples with subject as subject
    sparql = f"""
        DELETE WHERE {{
            ?s ?p <{subject["iri"]}> .
        }}
    """

    execute_update(sparql)

    return {
        "statusCode": 200,
        "body": json.dumps({"message": f"Subject removed: {subject['iri']}"}),
        "headers": {"Content-Type": "application/json"},
    }
