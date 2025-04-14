from smart_open import open
import json
from phebee.utils.aws import extract_body
from aws_lambda_powertools import Metrics, Logger, Tracer

from phebee.utils.sparql import (
    node_exists,
    subject_term_link_exists,
    create_subject_term_link,
    create_subject_term_evidence,
    get_subject,
)
from phebee.utils.eventbridge import (
    SUBJECT_TERM_LINK_CREATED,
    SUBJECT_TERM_EVIDENCE_ADDED,
    fire_event,
)

logger = Logger()
tracer = Tracer()
metrics = Metrics()


def create_error_response(status_code, message):
    logger.error(message)
    return {
        "statusCode": status_code,
        "body": json.dumps({"error": message}),
        "headers": {"Content-Type": "application/json"},
    }


def lambda_handler(event, context):
    logger.info(event)

    try:
        body = extract_body(event)

        if "detail-type" in body:
            body = body["detail"]

        if "evidence_s3_path" in body:
            evidence_s3_path = body["evidence_s3_path"]
            with open(evidence_s3_path) as evidence_input:
                evidence_list = json.load(evidence_input)
        else:
            evidence_list = body.get("evidence_list")
            if not evidence_list:
                return create_error_response(
                    400, "Missing 'evidence_list' in request body"
                )

        for evidence_item in evidence_list:
            project_id = evidence_item.get("project_id")
            project_subject_id = evidence_item.get("project_subject_id")
            term_iri = evidence_item.get("term_iri")
            evidence = evidence_item.get("evidence")

            if not project_id or not project_subject_id or not term_iri:
                return create_error_response(
                    400,
                    "Missing required evidence fields (project_id, project_subject_id, or term_iri)",
                )

            if not evidence:
                return create_error_response(400, "No evidence element was provided")

            existing_subject = get_subject(project_id, project_subject_id)
            if not existing_subject:
                return create_error_response(
                    400,
                    f"Subject does not exist for id {project_subject_id} in project {project_id}",
                )

            subject_iri = existing_subject["iri"]

            if not node_exists(term_iri):
                return create_error_response(
                    400, f"Term does not exist for IRI: {term_iri}"
                )

            link_exists_response = subject_term_link_exists(subject_iri, term_iri)

            if link_exists_response["link_exists"]:
                link_id = link_exists_response["link_id"]
                link_created = False
            else:
                link_created_response = create_subject_term_link(subject_iri, term_iri)

                if link_created_response["link_created"]:
                    link_id = link_created_response["link_id"]
                    link_created = True
                else:
                    return create_error_response(500, "Link creation failed")

            # Create evidence and attach to link
            create_subject_term_evidence(link_id, evidence)

            if link_created:
                fire_event(
                    SUBJECT_TERM_LINK_CREATED,
                    {
                        "subject_iri": subject_iri,
                        "term_iri": term_iri,
                        "evidence": evidence,
                    },
                )

            fire_event(
                SUBJECT_TERM_EVIDENCE_ADDED,
                {
                    "subject_iri": subject_iri,
                    "term_iri": term_iri,
                    "evidence": evidence,
                },
            )

        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "link_created": link_created,
                    "link_id": link_id,
                    "subject_iri": subject_iri,
                    "term_iri": term_iri,
                }
            ),
            "headers": {"Content-Type": "application/json"},
        }

    except Exception as e:
        logger.exception("Unexpected error while creating subject-term link")
        return create_error_response(500, f"Unexpected error: {str(e)}")
