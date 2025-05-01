import uuid
import json
from aws_lambda_powertools import Metrics, Logger, Tracer

from phebee.constants import PHEBEE
from phebee.utils.neptune import execute_update, execute_query
from phebee.utils.aws import get_current_timestamp, extract_body
from phebee.utils.eventbridge import SUBJECT_CREATED, SUBJECT_LINKED, fire_event
from phebee.utils.sparql import get_subject, project_exists

logger = Logger()
tracer = Tracer()
metrics = Metrics()


def create_error_response(status_code, message):
    return {
        "statusCode": status_code,
        "body": json.dumps({"subject_created": False, "error": message}),
        "headers": {"Content-Type": "application/json"},
    }


def lambda_handler(event, context):
    logger.info(event)
    body = extract_body(event)

    project_id = body.get("project_id")
    project_iri = f"{PHEBEE}/projects/{project_id}"
    project_subject_id = body.get("project_subject_id")
    project_subject_iri = f"{project_iri}/{project_subject_id}"

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

    # Ensure project exists
    if not project_exists(project_id):
        return create_error_response(400, f"No project found with ID: {project_id}")

    timestamp = get_current_timestamp()

    # Find or verify known subject
    if known_subject_iri:
        check_subject_query = f"""
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX phebee: <{PHEBEE}#>

        ASK WHERE {{
            GRAPH <http://ods.nationwidechildrens.org/phebee/subjects> {{
                <{known_subject_iri}> rdf:type phebee:Subject
            }}
        }}
        """
        if not execute_query(check_subject_query).get("boolean", False):
            return create_error_response(
                400, f"No subject found with IRI: {known_subject_iri}"
            )
        subject_iri = known_subject_iri

    elif known_project_id and known_project_subject_id:
        known_subject = get_subject(known_project_id, known_project_subject_id)
        if not known_subject:
            return create_error_response(
                400,
                f"No subject found with Project ID: {known_project_id} and Project Subject ID: {known_project_subject_id}",
            )
        subject_iri = known_subject["iri"]

    else:
        known_subject = get_subject(project_id, project_subject_id)
        if known_subject:
            subject_iri = known_subject["iri"]
        else:
            # Create new subject
            subject_uuid = uuid.uuid4()
            subject_iri = f"{PHEBEE}/subjects/{subject_uuid}"

            insert_sparql = f"""
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX phebee: <{PHEBEE}#>
            PREFIX dc: <http://purl.org/dc/terms/>

            INSERT DATA {{
                GRAPH <http://ods.nationwidechildrens.org/phebee/subjects> {{
                    <{subject_iri}> rdf:type phebee:Subject .
                }}

                GRAPH <{project_iri}> {{
                    <{subject_iri}> phebee:hasProjectSubjectId <{project_subject_iri}> .
                    <{project_subject_iri}> rdf:type phebee:ProjectSubjectId ;
                                            phebee:hasProject <{project_iri}> ;
                                            dc:created "{timestamp}"^^xsd:dateTime .
                }}
            }}
            """
            execute_update(insert_sparql)

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
                        "subject": {
                            "iri": subject_iri,
                            "projects": {project_id: project_subject_id},
                        },
                    }
                ),
                "headers": {"Content-Type": "application/json"},
            }

    # Link known subject to this project
    link_sparql = f"""
    PREFIX phebee: <{PHEBEE}#>
    PREFIX dc: <http://purl.org/dc/terms/>

    INSERT DATA {{
        GRAPH <{project_iri}> {{
            <{subject_iri}> phebee:hasProjectSubjectId <{project_subject_iri}> .
            <{project_subject_iri}> rdf:type phebee:ProjectSubjectId ;
                                    phebee:hasProject <{project_iri}> ;
                                    dc:created "{timestamp}"^^xsd:dateTime .
        }}
    }}
    """
    execute_update(link_sparql)

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
