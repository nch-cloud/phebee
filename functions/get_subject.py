from aws_lambda_powertools import Metrics, Logger, Tracer
from phebee.utils.iceberg import query_subject_by_id
from phebee.utils.aws import extract_body
from phebee.utils.dynamodb import get_current_term_source_version, get_subject_id, _get_table_name
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
    """Get subject info using project-scoped IRI (uses DynamoDB lookup)."""
    # Extract project_id and project_subject_id from IRI
    # Format: http://ods.nationwidechildrens.org/phebee/projects/{project_id}/{project_subject_id}
    parts = project_subject_iri.rstrip('/').split('/')
    if len(parts) < 2:
        logger.error(f"Invalid project_subject_iri format: {project_subject_iri}")
        return None

    project_subject_id = parts[-1]
    project_id = parts[-2]

    # Look up subject_id from DynamoDB mapping
    table_name = _get_table_name()
    subject_id = get_subject_id(table_name, project_id, project_subject_id)

    if not subject_id:
        logger.info(f"No subject found for project_id={project_id}, project_subject_id={project_subject_id}")
        return None

    # Get the full subject info with terms from Iceberg
    subject = get_subject_info_by_id(subject_id)

    # If subject exists in DynamoDB but has no terms yet, return it with empty terms
    if subject is None:
        subject = {
            "subject_iri": f"http://ods.nationwidechildrens.org/phebee/subjects/{subject_id}",
            "subject_id": subject_id,
            "terms": []
        }

    # Add project_subject_iri to response when queried via that path
    subject["project_subject_iri"] = project_subject_iri

    return subject


def get_subject_info_by_id(subject_id: str):
    """Get subject info using internal subject ID (direct to Iceberg)."""
    # Query Iceberg for terms using pre-aggregated subject_terms_by_subject table
    terms = query_subject_by_id(subject_id)

    if not terms:
        # No terms found - subject may not exist
        return None

    # Build subject info
    subject = {
        "subject_iri": f"http://ods.nationwidechildrens.org/phebee/subjects/{subject_id}",
        "subject_id": subject_id,
        "terms": terms
    }

    return subject


# Keep the old function name for backward compatibility
def get_subject_info(project_subject_iri: str):
    """Deprecated: Use get_subject_info_by_project_iri instead."""
    return get_subject_info_by_project_iri(project_subject_iri)
