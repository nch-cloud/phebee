from aws_lambda_powertools import Metrics, Logger, Tracer
from phebee.utils.neptune import execute_update
from phebee.utils.sparql import get_subject
from phebee.utils.aws import extract_body
import json

logger = Logger()
tracer = Tracer()
metrics = Metrics()


def lambda_handler(event, context):
    logger.info(event)

    body = extract_body(event)

    project_subject_iri = body.get("project_subject_iri")

    subject = get_subject(project_subject_iri)
    print(subject)

    if subject is None:
        return {
            "statusCode": 404,
            "body": json.dumps({"message": "Subject not found"}),
            "headers": {"Content-Type": "application/json"},
        }

    # TODO - Check for statements that will currently be orphaned.  Perhaps add a cascade flag to either prevent subject deletion or propagate it.

    # Remove triples with subject iri or project_subject_iri as subject or object
    for iri in [subject["subject_iri"], project_subject_iri]:
        execute_update(f"DELETE WHERE {{ <{iri}> ?p ?o . }}")
        execute_update(f"DELETE WHERE {{ ?s ?p <{iri}> . }}")

    # Delete cache entries and DynamoDB mappings
    from phebee.utils.sparql import delete_subject_from_cache_and_mappings
    delete_subject_from_cache_and_mappings(subject["subject_iri"])

    return {
        "statusCode": 200,
        "body": json.dumps({"message": f"Subject removed: {subject['subject_iri']}"}),
        "headers": {"Content-Type": "application/json"},
    }
