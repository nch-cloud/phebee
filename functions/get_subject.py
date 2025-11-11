from aws_lambda_powertools import Metrics, Logger, Tracer
from phebee.utils.sparql import get_term_links_with_evidence, get_subject
from phebee.utils.aws import extract_body
from phebee.utils.dynamodb import get_current_term_source_version
import json

logger = Logger()
tracer = Tracer()
metrics = Metrics()


def lambda_handler(event, context):
    logger.info(event)

    body = extract_body(event)

    project_subject_iri = body.get("project_subject_iri")

    subject = get_subject_info(project_subject_iri)
    logger.info(subject)

    if subject is None:
        response = {
            "statusCode": 404,
            "body": json.dumps({"message": "Subject not found"}),
            "headers": {"Content-Type": "application/json"},
        }
    else:
        response = {
            "statusCode": 200,
            "body": json.dumps(subject),
            "headers": {"Content-Type": "application/json"},
        }

    logger.info(response)

    return response


def get_subject_info(project_subject_iri: str):
    # Retrieve the IRI for the subject node in our graph associated to the given project with the given project-subject id
    subject = get_subject(project_subject_iri)
    if subject is None:
        return None
    
    # Query for subject-term links with evidence counts only (lightweight)
    hpo_version = get_current_term_source_version("hpo")
    mondo_version = get_current_term_source_version("mondo")

    # Use the lightweight version that only returns counts
    from phebee.utils.sparql import get_term_links_with_counts
    terms = get_term_links_with_counts(subject["subject_iri"], hpo_version, mondo_version)
    subject["terms"] = terms

    return subject
