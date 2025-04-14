from aws_lambda_powertools import Metrics, Logger, Tracer
from phebee.utils.sparql import get_subject_term_info, get_subject
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

    subject = get_subject_info(project_id, project_subject_id)
    
    return {
        "statusCode": 200,
        "body": json.dumps(subject),
        "headers": {"Content-Type": "application/json"},
    }


def get_subject_info(project_id: str, project_subject_id: str):
    # Retrieve the IRI for the subject node in our graph associated to the given project with the given project-subject id
    subject = get_subject(project_id, project_subject_id)

    # Query for subject-term links, their terms, and evidence
    terms = get_subject_term_info(subject["iri"])
    subject["terms"] = terms

    return subject
