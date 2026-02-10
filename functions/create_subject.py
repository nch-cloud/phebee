import json
from aws_lambda_powertools import Metrics, Logger, Tracer

from phebee.utils.aws import extract_body
from phebee.utils.eventbridge import SUBJECT_CREATED, SUBJECT_LINKED, fire_event
from phebee.utils.sparql import (
    get_subject,
    project_exists,
    subject_exists,
    create_subject,
    link_subject_to_project,
)
from phebee.utils.dynamodb import _get_table_name
import boto3

logger = Logger()
tracer = Tracer()
metrics = Metrics()


def create_error_response(status_code, message):
    return {
        "statusCode": status_code,
        "body": json.dumps({"subject_created": False, "error": message}),
        "headers": {"Content-Type": "application/json"},
    }


def create_dynamodb_mapping(project_id: str, project_subject_id: str, subject_iri: str):
    """Create DynamoDB mapping for project-subject relationship"""
    table_name = _get_table_name()
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)

    # Extract UUID from IRI
    subject_id = subject_iri.split('/subjects/')[-1]

    try:
        with table.batch_writer() as batch:
            # Direction 1: Project → Subject
            batch.put_item(Item={
                'PK': f'PROJECT#{project_id}',
                'SK': f'SUBJECT#{project_subject_id}',
                'subject_id': subject_id  # Store just UUID
            })

            # Direction 2: Subject → Project
            batch.put_item(Item={
                'PK': f'SUBJECT#{subject_id}',
                'SK': f'PROJECT#{project_id}#SUBJECT#{project_subject_id}'
            })
    except Exception as e:
        logger.error(f"Failed to create DynamoDB mapping: {e}")
        # Don't fail the whole operation, just log the error


def lambda_handler(event, context):
    logger.info(event)
    body = extract_body(event)

    project_id = body.get("project_id")
    project_subject_id = body.get("project_subject_id")

    known_project_id = body.get("known_project_id")
    known_project_subject_id = body.get("known_project_subject_id")
    known_subject_iri = body.get("known_subject_iri")

    # Validate input consistency
    if known_project_id and not known_project_subject_id:
        return create_error_response(
            400,
            "If 'known_project_id' is provided, 'known_project_subject_id' is required.",
        )
    if known_project_id and known_subject_iri:
        return create_error_response(
            400, "'known_project_id' and 'known_subject_iri' cannot both be provided."
        )

    if not project_exists(project_id):
        return create_error_response(400, f"No project found with ID: {project_id}")

    # Determine subject IRI
    if known_subject_iri:
        if not subject_exists(known_subject_iri):
            return create_error_response(
                400, f"No subject found with IRI: {known_subject_iri}"
            )
        subject_iri = known_subject_iri

    elif known_project_id and known_project_subject_id:
        known_project_subject_iri = f"http://ods.nationwidechildrens.org/phebee/projects/{known_project_id}/{known_project_subject_id}"
        known_subject = get_subject(known_project_subject_iri)
        if not known_subject:
            return create_error_response(
                400,
                f"No subject found with Project ID: {known_project_id} and Project Subject ID: {known_project_subject_id}",
            )
        subject_iri = known_subject["subject_iri"]

    else:
        known_project_subject_iri = f"http://ods.nationwidechildrens.org/phebee/projects/{project_id}/{project_subject_id}"
        known_subject = get_subject(known_project_subject_iri)
        if known_subject:
            subject_iri = known_subject["subject_iri"]
        else:
            subject_iri = create_subject(project_id, project_subject_id)
            # Link new subject to project
            link_subject_to_project(subject_iri, project_id, project_subject_id)
            # Create DynamoDB mapping for new subject
            create_dynamodb_mapping(project_id, project_subject_id, subject_iri)
            subject_data = {
                "iri": subject_iri,
                "projects": {project_id: project_subject_id},
            }
            fire_event(SUBJECT_CREATED, subject_data)
            return {
                "statusCode": 200,
                "body": json.dumps(
                    {
                        "subject_created": True,
                        "subject": subject_data,
                    }
                ),
                "headers": {"Content-Type": "application/json"},
            }

    # Link existing subject to project (if needed)
    link_subject_to_project(subject_iri, project_id, project_subject_id)
    # Create DynamoDB mapping for existing subject linked to new project
    create_dynamodb_mapping(project_id, project_subject_id, subject_iri)
    fire_event(
        SUBJECT_LINKED,
        {"subject_iri": subject_iri, "projects": {project_id: project_subject_id}},
    )

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "subject_created": False,
                "subject": {
                    "iri": subject_iri,
                    "projects": {project_id: project_subject_id},
                },
            }
        ),
        "headers": {"Content-Type": "application/json"},
    }
