from aws_lambda_powertools import Metrics, Logger, Tracer
from phebee.utils.sparql import get_subject, get_term_links_with_counts
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
    subject_id = body.get("subject_id")

    if project_subject_iri:
        subject = get_subject_info_by_project_iri(project_subject_iri)
    elif subject_id:
        subject = get_subject_info_by_id(subject_id)
    else:
        response = {
            "statusCode": 400,
            "body": json.dumps({"message": "Must provide either project_subject_iri or subject_id"}),
            "headers": {"Content-Type": "application/json"},
        }
        logger.info(response)
        return response

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


def get_subject_info_by_project_iri(project_subject_iri: str):
    """Get subject info using project-scoped IRI (requires SPARQL lookup)."""
    # Retrieve the IRI for the subject node in our graph associated to the given project with the given project-subject id
    subject = get_subject(project_subject_iri)
    if subject is None:
        return None
    
    # Extract subject_id from the subject IRI
    subject_id = subject["subject_iri"].split("/")[-1]
    
    # Get the full subject info with terms
    return get_subject_info_by_id(subject_id, base_subject=subject)


def get_subject_info_by_id(subject_id: str, base_subject: dict = None):
    """Get subject info using internal subject ID (direct to Iceberg)."""
    # Query Iceberg for term links with evidence counts
    hpo_version = get_current_term_source_version("hpo")
    mondo_version = get_current_term_source_version("mondo")

    # Use the new Iceberg-based version with subject_id
    terms = get_term_links_with_counts(subject_id, hpo_version=hpo_version, mondo_version=mondo_version)
    
    if base_subject:
        # We already have subject metadata from SPARQL lookup
        subject = base_subject.copy()
        # Ensure subject_id is included for consistency
        if "subject_id" not in subject:
            subject["subject_id"] = subject["subject_iri"].split("/")[-1]
    else:
        # We only have subject_id, so create minimal subject info
        subject = {
            "subject_iri": f"http://ods.nationwidechildrens.org/phebee/subjects/{subject_id}",
            "subject_id": subject_id
        }
    
    subject["terms"] = terms
    return subject


# Keep the old function name for backward compatibility
def get_subject_info(project_subject_iri: str):
    """Deprecated: Use get_subject_info_by_project_iri instead."""
    return get_subject_info_by_project_iri(project_subject_iri)
