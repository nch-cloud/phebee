import uuid
import json
from aws_lambda_powertools import Metrics, Logger, Tracer

from phebee.constants import PHEBEE
from phebee.utils.neptune import execute_update, execute_query
from phebee.utils.aws import get_current_timestamp, extract_body
from phebee.utils.eventbridge import SUBJECT_CREATED, SUBJECT_LINKED, fire_event
from phebee.utils.sparql import get_subject


logger = Logger()
tracer = Tracer()
metrics = Metrics()


def create_error_response(status_code, message):
    return {
        "statusCode": status_code,
        "body": json.dumps({
            "subject_created": False,
            "error": message
        }),
        "headers": {
            "Content-Type": "application/json"
        }
    }


def lambda_handler(event, context):
    logger.info(event)

    body = extract_body(event)

    project_id = body.get("project_id")
    project_subject_id = body.get("project_subject_id")
    known_project_id = body.get("known_project_id")
    known_project_subject_id = body.get("known_project_subject_id")
    known_subject_iri = body.get("known_subject_iri")

    # Input validation
    if known_project_id and not known_project_subject_id:
        return create_error_response(
            400,
            "If 'known_project_id' is provided, 'known_project_subject_id' is required.",
        )
    if known_project_id and known_subject_iri:
        return create_error_response(
            400, "'known_project_id' and 'known_subject_iri' cannot both be provided."
        )

    # Check if the project_id exists in the database
    check_project_exists_query = f"""
        PREFIX phebee: <{PHEBEE}>
        ASK WHERE {{ <{PHEBEE}/projects/{project_id}> rdf:type phebee:\#Project }}
    """
    project_exists = execute_query(check_project_exists_query)
    project_exists = project_exists["boolean"]
    if not project_exists:
        return create_error_response(400, f"No project found with ID: {project_id}")

    current_timestamp = get_current_timestamp()

    # Handling existing subject scenarios
    if known_subject_iri:
        # Check if the known_subject_iri exists in the database
        check_subject_exists_query = f"""
            PREFIX phebee: <{PHEBEE}>
            ASK WHERE {{ <{known_subject_iri}> rdf:type phebee:\#Subject }}
        """
        subject_exists = execute_query(check_subject_exists_query)
        subject_exists = subject_exists["boolean"]
        if not subject_exists:
            return create_error_response(
                400, f"No subject found with IRI: {known_subject_iri}"
            )
        existing_subject = {"iri": known_subject_iri}
    elif known_project_id and known_project_subject_id:
        existing_subject = get_subject(known_project_id, known_project_subject_id)
        if not existing_subject:
            return create_error_response(
                400,
                f"No subject found with Project ID: {known_project_id} and Project Subject ID: {known_project_subject_id}",
            )
    else:
        existing_subject = get_subject(project_id, project_subject_id)

    # If an existing subject is found, link it to the new project
    # #<{subject_iri}> rdf:type phebee:\#Subject .
    if existing_subject:
        link_existing_subject_sparql = f"""
            PREFIX phebee: <{PHEBEE}>
            PREFIX dcterms: <http://purl.org/dc/terms/>

            INSERT DATA
            {{
                GRAPH <{PHEBEE}/projects/{project_id}>
                {{
                    <{existing_subject['iri']}> phebee:\#hasProjectSubjectId phebee:\/projects\/{project_id}\#{project_subject_id} .
                    phebee:\/projects\/{project_id}\#{project_subject_id} rdf:type phebee:\#ProjectSubjectId .
                    phebee:\/projects\/{project_id}\#{project_subject_id} phebee:\#hasProject phebee:\/projects\/{project_id} .
                    phebee:\/projects\/{project_id}\#{project_subject_id} dcterms:created "{current_timestamp}"
                }}
            }}
        """
        execute_update(link_existing_subject_sparql)

        subject_data = {
            "id": existing_subject["iri"],
            "projects": {
                f"{PHEBEE}/projects/{project_id}": f"{PHEBEE}/projects/{project_id}#{project_subject_id}"
            },
        }

        # Fire an EventBridge event for the subject being linked
        fire_event(SUBJECT_LINKED, subject_data)

        return {
            "statusCode": 200,
            "body": json.dumps({
                "subject_created": False,
                "subject": subject_data
            }),
            "headers": {
                "Content-Type": "application/json"
            }
        }

    # If no existing subject, create a new one
    subject_uuid = uuid.uuid4()
    subject_iri = f"{PHEBEE}/subjects#{subject_uuid}"

    # Add the link from our new subject to a project-subject id node linked to
    # the desired project in the project graph, and timestamp the creation
    add_project_subject_id_sparql = f"""
        PREFIX phebee: <{PHEBEE}>
        PREFIX dcterms: <http://purl.org/dc/terms/>

        INSERT DATA
        {{
            GRAPH <{PHEBEE}/projects/{project_id}>
            {{
                <{subject_iri}> rdf:type phebee:\#Subject .
                <{subject_iri}> phebee:\#hasProjectSubjectId phebee:\/projects\/{project_id}\#{project_subject_id} .
                phebee:\/projects\/{project_id}\#{project_subject_id} rdf:type phebee:\#ProjectSubjectId .
                phebee:\/projects\/{project_id}\#{project_subject_id} phebee:\#hasProject phebee:\/projects\/{project_id} .
                phebee:\/projects\/{project_id}\#{project_subject_id} dcterms:created "{current_timestamp}"
            }}
        }}
    """

    add_project_subject_id_result = execute_update(add_project_subject_id_sparql)

    logger.info(add_project_subject_id_result)

    subject_data = {
        "id": subject_iri,
        "projects": {
            f"{PHEBEE}/projects/{project_id}": f"{PHEBEE}/projects/{project_id}#{project_subject_id}"
        },
    }

    # Fire an EventBridge event for the subject being created
    fire_event(SUBJECT_CREATED, subject_data)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "subject_created": True,
            "subject": subject_data
        }),
        "headers": {
            "Content-Type": "application/json"
        }
    }
