import json
from aws_lambda_powertools import Metrics, Logger, Tracer

from phebee.utils.sparql import get_subjects
from phebee.utils.aws import parse_s3_path, get_client, extract_body
from phebee.utils.dynamodb import get_current_term_source_version

logger = Logger()
tracer = Tracer()
metrics = Metrics()

s3_client = get_client("s3")


# Build a phenopacket given the input data of a single subject
def lambda_handler(event, context):
    """
    Lambda handler that processes SPARQL queries for a project.

    :param event: The event object containing parameters for the query.
    :param context: The AWS Lambda context object.
    :return: Response from the SPARQL query or error message.
    """

    logger.info(f"Event: {event}")

    body = extract_body(event)

    project_id = body.get("project_id")
    if not project_id:
        raise ValueError("Missing required parameter 'project_id'.")

    # Optional parameters with defaults or fallbacks
    project_subject_ids = body.get("project_subject_ids", None)
    term_iri = body.get("term_iri", None)
    term_source = body.get("term_source", "hpo")
    term_source_version = body.get(
        "term_source_version", get_current_term_source_version(term_source)
    )
    return_excluded_terms = body.get("return_excluded_terms", False)
    include_descendants = body.get("include_descendants", False)
    include_phenotypes = body.get("include_phenotypes", False)
    include_evidence = body.get("include_evidence", False)
    optional_evidence = body.get("optional_evidence", None)
    return_raw_json = body.get("return_raw_json", False)

    flat_results = get_subjects(
        project_id=project_id,
        project_subject_ids=project_subject_ids,
        term_iri=term_iri,
        term_source=term_source,
        term_source_version=term_source_version,
        return_excluded_terms=return_excluded_terms,
        include_descendants=include_descendants,
        include_phenotypes=include_phenotypes,
        include_evidence=include_evidence,
        optional_evidence=optional_evidence,
        return_raw_json=return_raw_json,
    )

    if "output_s3_path" in body:
        bucket, key = parse_s3_path(body["output_s3_path"])
        json_data = json.dumps(flat_results, indent=2)
        s3_client.put_object(Bucket=bucket, Key=key, Body=json_data)

        return {
            "statusCode": 200,
            "body": json.dumps(
                {"s3_path": f"s3://{bucket}/{key}", "n_subjects": len(flat_results)}
            ),
        }

    else:
        return {
            "statusCode": 200,
            "body": json.dumps({"body": flat_results, "n_subjects": len(flat_results)}),
        }
