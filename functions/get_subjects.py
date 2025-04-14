from aws_lambda_powertools import Metrics, Logger, Tracer
from phebee.utils.sparql import get_subjects_for_project, get_subject_term_info
from phebee.utils.dynamodb import get_current_term_source_version
from phebee.utils.aws import extract_body
import json

logger = Logger()
tracer = Tracer()
metrics = Metrics()


def lambda_handler(event, context):
    logger.info(event)

    body = extract_body(event)

    project_id = body.get("project_id")
    term_source = body.get("term_source")
    term_source_version = body.get("term_source_version")
    term_iri = body.get("term_iri")
    include_terms = body.get("include_terms", "false").lower() == "true"

    if not term_source_version:
        term_source_version = get_current_term_source_version(term_source)
        logger.info(
            f"Retrieved current source version for {term_source}: {term_source_version}"
        )

    return {
        "statusCode": 200,
        "body": json.dumps(
            get_subjects(
                project_id, term_source, term_source_version, term_iri, include_terms
            )
        ),
        "headers": {"Content-Type": "application/json"},
    }


@tracer.capture_method
def get_subjects(
    project_id: str,
    term_source: str = None,
    term_source_version: str = None,
    term_iri: str = None,
    include_terms: bool = False,
):
    # Retrieve info including the subject node IRI for all subjects that have a link to the given project
    subjects = get_subjects_for_project(
        project_id,
        term_source=term_source,
        term_source_version=term_source_version,
        term_iri=term_iri,
    )

    if include_terms:
        # TODO - This could probably be made more efficient than getting info for each term as a separate query
        for subject in subjects:
            subject["terms"] = get_subject_term_info(subject["iri"])

    return subjects
