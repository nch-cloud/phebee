import json
import gzip
import base64
from aws_lambda_powertools import Metrics, Logger, Tracer
from phebee.utils.sparql import get_subjects
from phebee.utils.aws import parse_s3_path, get_client, extract_body
from phebee.utils.dynamodb import get_current_term_source_version
from phebee.utils.phenopackets import subjects_to_phenopackets, zip_phenopackets
from phebee.constants import PHEBEE

logger = Logger()
tracer = Tracer()
metrics = Metrics()

s3_client = get_client("s3")


def lambda_handler(event, context):
    logger.info("Event: %s", event)

    body = extract_body(event)

    project_id = body.get("project_id")
    if not project_id:
        raise ValueError("Missing required parameter 'project_id'.")

    project_iri = f"{PHEBEE}/projects/{project_id}"

    output_type = body.get("output_type", "json")

    term_iri = body.get("term_iri")
    term_source = body.get("term_source", "hpo")
    term_source_version = body.get(
        "term_source_version", get_current_term_source_version(term_source)
    )

    hpo_version = get_current_term_source_version("hpo")
    mondo_version = get_current_term_source_version("mondo")

    project_subject_ids = body.get("project_subject_ids")
    
    # Qualifier filtering parameter
    include_qualified = body.get("include_qualified", False)
    
    # Pagination parameters
    limit = body.get("limit", 200)
    cursor = body.get("cursor")

    result = get_subjects(
        project_iri=project_iri,
        hpo_version=hpo_version,
        mondo_version=mondo_version,
        limit=limit,
        cursor=cursor,
        term_iri=term_iri,
        term_source=term_source,
        term_source_version=term_source_version,
        project_subject_ids=project_subject_ids,
        include_qualified=include_qualified,
    )
    
    # Extract subjects and pagination info
    subject_data = result["subjects"]
    pagination = result["pagination"]

    if "phenopacket" in output_type:
        subject_data = subjects_to_phenopackets(subject_data, project_iri, hpo_version, mondo_version)

    if output_type == "phenopacket_zip":
        s3_content = zip_phenopackets(subject_data)
    else:
        s3_content = json.dumps({"subjects": subject_data, "pagination": pagination}, indent=2)

    if "output_s3_path" in body:
        bucket, key = parse_s3_path(body["output_s3_path"])
        s3_client.put_object(Bucket=bucket, Key=key, Body=s3_content)

        return {
            "statusCode": 200,
            "body": json.dumps(
                {"s3_path": f"s3://{bucket}/{key}", "n_subjects": len(subject_data), "pagination": pagination}
            ),
        }

    # Compress response for direct API calls
    response_data = {"body": subject_data, "n_subjects": len(subject_data), "pagination": pagination}
    json_str = json.dumps(response_data)
    
    # Compress the JSON response
    compressed_data = gzip.compress(json_str.encode('utf-8'))
    
    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json",
            "Content-Encoding": "gzip"
        },
        "body": base64.b64encode(compressed_data).decode('utf-8'),
        "isBase64Encoded": True
    }
