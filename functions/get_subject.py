from aws_lambda_powertools import Metrics, Logger, Tracer
from phebee.utils.sparql import get_term_links_for_node, get_subject
from phebee.utils.aws import extract_body
from phebee.utils.dynamodb import get_current_term_source_version
import json

logger = Logger()
tracer = Tracer()
metrics = Metrics()


def lambda_handler(event, context):
    logger.info(event)

    body = extract_body(event)

    project_id = body.get("project_id")
    project_subject_iri = body.get("project_subject_iri")

    subject = get_subject_info(project_id, project_subject_iri)
    
    return {
        "statusCode": 200,
        "body": json.dumps(subject),
        "headers": {"Content-Type": "application/json"},
    }


def get_subject_info(project_id: str, project_subject_iri: str):
    # Retrieve the IRI for the subject node in our graph associated to the given project with the given project-subject id
    subject = get_subject(project_id, project_subject_iri)

    # Query for subject-term links, their terms, and evidence
    hpo_version = get_current_term_source_version("hpo")
    mondo_version = get_current_term_source_version("mondo")

    # TODO: Maybe the versions should be in a dictionary?
    terms = get_term_links_for_node(subject["iri"], hpo_version, mondo_version)
    subject["terms"] = terms

    return subject
